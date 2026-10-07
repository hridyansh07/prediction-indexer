import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.prepare_context import universe_from_environment, main
from replay.preparation import load_snapshot, encoded
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document


class PrepareEndpointTests(unittest.TestCase):
    def test_literal_dotenv_and_environment_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"synthetic.env"
            path.write_text("IGNORED_SYNTHETIC=anything\nexport UNIVERSE_BASE_URL='https://universe.example/base' # comment\n")
            source=universe_from_environment(env_file=path,environ={})
            self.assertEqual(source.base_url,"https://universe.example/base")
            with patch.object(source,"_get",return_value=document()) as get:
                source.outcomes("bundle-1")
                get.assert_called_once_with("https://universe.example/base/v1/bundles/bundle-1/outcomes")
            self.assertEqual(universe_from_environment(env_file=path,environ={"UNIVERSE_BASE_URL":"https://export.example"}).base_url,"https://export.example")

    def test_missing_invalid_and_duplicate_fail_without_url_echo(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/"synthetic.env"
            with self.assertRaisesRegex(ValueError,"required"):
                universe_from_environment(env_file=path,environ={})
            for url in ("", "host:8080", "ftp://host", "https://u:p@host", "https://host?x=1",
                        "https://host#fragment", "https://host:bad", "https://host:0", "https://host with-space", "https://"):
                with self.subTest(url=url), self.assertRaises(ValueError) as err:
                    universe_from_environment(env_file=path,environ={"UNIVERSE_BASE_URL":url})
                if url:
                    self.assertNotIn(url,str(err.exception))
            path.write_text("UNIVERSE_BASE_URL=https://one.example\nUNIVERSE_BASE_URL=https://two.example\n")
            with self.assertRaisesRegex(ValueError,"duplicate"):
                universe_from_environment(env_file=path,environ={})

    def test_cli_loads_dotenv_and_prepares_v2_with_one_client(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); env=root/"synthetic.env"; cfg=root/"prepare.json"; output=root/"context"
            env.write_text('UNIVERSE_BASE_URL="https://universe.example"\n')
            cfg.write_bytes(encoded(config()))
            calls=[]
            def request(source,url):
                calls.append(url)
                return document() if url.endswith("/outcomes") else detail()
            with patch.dict("os.environ",{},clear=True), patch("replay.preparation.UniverseHTTP._get",request):
                self.assertEqual(main([str(cfg),str(output),"--env-file",str(env)]),0)
            snapshot=load_snapshot(output)
            self.assertEqual(snapshot["version"],2)
            self.assertEqual(snapshot["outcomes"]["provider"],"universe")
            self.assertEqual(len(calls),2)
            self.assertTrue(all(url.startswith("https://universe.example/v1/") for url in calls))
