import filecmp
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.venezuela_3w.venezuela_3w import Venezuela3w


class TestVenezuela3w:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
        def read_from_hdx(dataset_name):
            return Dataset.load_from_json(
                join(
                    "tests",
                    "fixtures",
                    "input",
                    f"dataset-{dataset_name}.json",
                )
            )

        monkeypatch.setattr(
            Dataset, "read_from_hdx", staticmethod(read_from_hdx)
        )

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "venezuela_3w", "config")

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
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }

                assert filecmp.cmp(
                    join(tempdir, "VEN_5W_2024.csv"),
                    join(fixtures_dir, "VEN_5W_2024.csv"),
                )
