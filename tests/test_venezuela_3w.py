from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.venezuela_3w.venezuela_3w import Venezuela3w


class TestVenezuela3w:
    def test_venezuela_3w(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "Test_venezuela_3w",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                venezuela_3w = Venezuela3w(configuration, retriever)
                venezuela_3w.get_data(2024)
                assert len(venezuela_3w.data) == 3000

                dataset = venezuela_3w.generate_dataset(2024)
                assert (
                    dataset.get_time_period()["enddate_str"]
                    == "2024-07-01T23:59:59+00:00"
                )

                resources = dataset.get_resources()
                assert len(resources) == 5

                assert resources[0] == {
                    "name": "VEN_5W_2024.csv",
                    "description": "5W data from Venezuela (2024)",
                    "p_coded": True,
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                    "dataset_preview_enabled": "True",
                }

                assert_files_same(
                    join(tempdir, "VEN_5W_2024.csv"),
                    join(fixtures_dir, "VEN_5W_2024.csv"),
                )
