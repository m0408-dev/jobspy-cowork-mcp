"""Operator-issued revocable credentials. Plaintext keys are never stored in SQLite."""
from contextlib import closing
import asyncio
import hashlib
import os
import re
import secrets
import sqlite3
from pathlib import Path
from fastmcp.server.auth import TokenVerifier, AccessToken
from fastmcp.server.dependencies import get_access_token


class Credentials(TokenVerifier):
    def __init__(self, path, owner_token=''):
        super().__init__()
        self.path = str(path)
        self.owner_token = owner_token or secrets.token_urlsafe(48)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS credentials (digest TEXT PRIMARY KEY, subject TEXT NOT NULL, active INTEGER NOT NULL)')

    def issue(self, subject):
        if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}', subject) or subject == 'owner':
            raise ValueError('Use a unique user name other than owner')
        token = secrets.token_urlsafe(48)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO credentials VALUES (?,?,1)', (hashlib.sha256(token.encode()).hexdigest(), subject))
        return token

    def revoke(self, subject):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE credentials SET active=0 WHERE subject=?', (subject,))

    def subject(self, token):
        if secrets.compare_digest(token, self.owner_token):
            return 'owner'
        with closing(sqlite3.connect(self.path)) as db, db:
            row = db.execute('SELECT subject FROM credentials WHERE digest=? AND active=1',
                             (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
        return row[0] if row else None

    async def verify_token(self, token):
        subject = await asyncio.to_thread(self.subject, token)
        if subject:
            return AccessToken(token=token, client_id=subject, scopes=[], claims={'sub': subject})
        return None


def current_owner():
    token = get_access_token()
    # Local stdio/in-process use is the operator. HTTP always passes the verifier.
    return token.claims['sub'] if token else 'owner'


class LegacyOwnerRoute:
    """Keep an existing private owner URL working; public /mcp/ requires a key."""
    def __init__(self, app, path, credentials):
        self.app, self.path, self.credentials = app, path, credentials

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http' and self.path != '/mcp/' and scope['path'] == self.path:
            scope = dict(scope, path='/mcp/', raw_path=b'/mcp/')
            headers = list(scope.get('headers', []))
            if not any(k.lower() == b'authorization' for k, v in headers):
                headers.append((b'authorization', ('Bearer '+self.credentials.owner_token).encode()))
            scope['headers'] = headers
        await self.app(scope, receive, send)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['issue', 'revoke'])
    parser.add_argument('subject')
    parser.add_argument('--output', help='New private file for the issued key')
    args = parser.parse_args()
    credentials = Credentials(os.getenv('AUTH_DB', '/app/data/access.sqlite'))
    if args.action == 'revoke':
        credentials.revoke(args.subject)
    else:
        if not args.output:
            parser.error('issue requires --output; keys are never printed')
        with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
            f.write(credentials.issue(args.subject))
    print('Credential operation complete')
