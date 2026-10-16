"""No-network upstream fixture through the real deployed MCP HTTP transport."""
import asyncio
import json
import os
import secrets
from fastmcp import Client
from v4_server import STORE,VERSION

async def main():
    url='http://127.0.0.1:8000'+os.environ.get('MCP_HTTP_PATH','/mcp/')
    async with Client(url,auth=os.environ.get('MCP_AUTH_TOKEN') or None) as client:
        tools=await client.list_tools()
        assert len(tools)==9
        required={'start_job_search','get_search_status','find_jobs','read_job','get_recovery_tasks','record_recovery','pause_search','resume_search','list_job_sources'}
        assert {t.name for t in tools}==required
        result=await client.call_tool('list_job_sources',{})
        assert result.structured_content['version']==VERSION=='4.1.1'
        rid=STORE.create({'queries':[]},[])
        title='Transport'+chr(0x2028)+'roundtrip'
        prefix='https://example.com/probe/'+secrets.token_hex(12)+'/'
        with STORE.connect() as db:
            STORE._save_jobs(db,rid,[{'title':title,'job_url':prefix+str(i),'description':'Fixture description','source':'fixture'} for i in range(65)])
        try:
            offset=0;ids=[]
            while offset is not None:
                page=(await client.call_tool('find_jobs',{'search_id':rid,'offset':offset,'limit':30})).structured_content
                assert page['matching_jobs']==65
                assert all(j['title']==title for j in page['jobs'])
                ids.extend(j['id'] for j in page['jobs'])
                offset=page['next_offset']
            assert len(set(ids))==65
            empty=(await client.call_tool('find_jobs',{'search_id':rid,'query':'absentfixture'})).structured_content
            assert empty['matching_jobs']==0
            detail=(await client.call_tool('read_job',{'search_id':rid,'job_id':ids[0]})).structured_content
            assert detail['description']=='Fixture description'
        finally:
            with STORE.connect() as db:
                db.execute('DELETE FROM job_text WHERE rowid IN (SELECT fts_rowid FROM job_text_rows WHERE id IN (SELECT job_id FROM run_jobs WHERE run_id=?))',(rid,))
                db.execute('DELETE FROM job_text_rows WHERE id IN (SELECT job_id FROM run_jobs WHERE run_id=?)',(rid,))
                db.execute('DELETE FROM jobs WHERE id IN (SELECT job_id FROM run_jobs WHERE run_id=?)',(rid,))
                db.execute('DELETE FROM run_jobs WHERE run_id=?',(rid,))
                db.execute('DELETE FROM runs WHERE id=?',(rid,))
        print('MCP HTTP OK: v4.1.1, nine structured tools, paging, Unicode, full text, details')

asyncio.run(main())
