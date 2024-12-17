#!/usr/bin/python
"""venezuela_3w scraper"""

import logging
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Venezuela3w:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = retriever.temp_dir
        self.data = []

    def get_data(self, year: int) -> None:
        base_url = self._configuration["base_url"].replace("YYYY", str(year))
        json = self._retriever.download_json(base_url)
        pages = json["total_pages"]
        self.data = json["data"]
        for page in range(2, pages + 1):
            json_url = f"{base_url}?page={page}"
            json = self._retriever.download_json(json_url)
            self.data.extend(json["data"])

    def generate_dataset(self, year: int) -> Optional[Dataset]:
        dataset = Dataset.read_from_hdx(self._configuration["dataset_name"])
        dataset_time_period = dataset.get_time_period()
        start_date = dataset_time_period["startdate"]
        end_date = dataset_time_period["enddate"]
        data_end_dates = [parse_date(row["report_month"]) for row in self.data]
        data_end_date = max(data_end_dates)
        if data_end_date > end_date:
            dataset.set_time_period(start_date, data_end_date)

        resourcedata = {
            "name": f"VEN_5W_{year}.csv",
            "description": f"5W data from Venezuela ({year})",
        }
        hxltags = self._configuration["hxltags"]
        headers = list(hxltags.keys())
        dataset.generate_resource_from_iterable(
            headers,
            self.data,
            hxltags,
            self._temp_dir,
            f"VEN_5W_{year}.csv",
            resourcedata,
            encoding="utf-8-sig",
        )

        resources = dataset.get_resources()
        from_index = None
        to_index = 0
        for i, resource in enumerate(resources):
            resource_name = resource["name"]
            if resource_name == f"VEN_5W_{year}.csv":
                from_index = i
        resource = resources.pop(from_index)
        if from_index < to_index:
            # to index was calculated while element was in front
            to_index -= 1
        resources.insert(to_index, resource)

        return dataset
