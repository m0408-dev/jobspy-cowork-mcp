import json
import unittest
from unittest.mock import AsyncMock, patch
import server
import sources

class GenericTests(unittest.IsolatedAsyncioTestCase):
    async def test_any_occupation_is_forwarded_unchanged(self):
        for term in ["Einzelhandel", "Koch", "Buchhaltung", "IT Support", "看護師"]:
            api=AsyncMock(return_value=([],{"queries_used":[term],"per_source":{}}))
            boards=AsyncMock(return_value=([],{}))
            with patch("server.fetch_sources",api),patch("server._jobspy_batch",boards),patch("server._snapshot",return_value="ok"):
                await server.search_all_jobs(term)
            self.assertEqual(api.call_args.args[1],term)
            self.assertEqual(boards.call_args.args[0],[term])
            self.assertEqual(boards.call_args.args[3],server.GERMANY_BOARDS)

    def test_no_hidden_synonyms(self):
        self.assertEqual(sources.expand_terms("Koch"),["Koch"])
        self.assertEqual(sources.expand_terms("Support",["support","Einzelhandel"]),["Support","Einzelhandel"])
        self.assertFalse(hasattr(sources,"_SYNONYMS"))
        self.assertFalse(hasattr(sources,"_TAIL_FAMILIES"))
        with self.assertRaisesRegex(ValueError,"explicit search_terms"):
            sources.expand_terms("Koch",expand=True)

    async def test_explicit_api_list_does_not_add_boards(self):
        api=AsyncMock(return_value=([],{"queries_used":["Koch"],"per_source":{}}))
        boards=AsyncMock(return_value=([],{}))
        with patch("server.fetch_sources",api),patch("server._jobspy_batch",boards),patch("server._snapshot",return_value="ok"):
            await server.search_all_jobs("Koch",sources=["arbeitnow"])
        boards.assert_not_called()

    async def test_explicit_board_selection_and_opt_out(self):
        api=AsyncMock(return_value=([],{"queries_used":["Koch"],"per_source":{}}))
        boards=AsyncMock(return_value=([],{}))
        with patch("server.fetch_sources",api),patch("server._jobspy_batch",boards),patch("server._snapshot",return_value="ok"):
            await server.search_all_jobs("Koch",sources=[],jobspy_sites=["indeed"])
            self.assertEqual(boards.call_args.args[3],["indeed"])
            boards.reset_mock()
            await server.search_all_jobs("Koch",include_jobspy=False)
            boards.assert_not_called()

    async def test_remote_helper_is_broad_not_two_sources(self):
        target=AsyncMock(return_value="ok")
        with patch("server.search_all_jobs",target):
            await server.search_remote_jobs("Buchhaltung")
        self.assertEqual(target.call_args.kwargs["search_term"],"Buchhaltung")
        self.assertTrue(target.call_args.kwargs["remote_only"])
        self.assertIsNone(target.call_args.kwargs["sources"])

    def test_catalog_matches_actual_default(self):
        catalog=json.loads(server.list_job_sources())
        self.assertEqual(catalog["defaults"]["germany"],["arbeitsagentur","arbeitnow",*server.GERMANY_BOARDS])

if __name__=="__main__": unittest.main()
