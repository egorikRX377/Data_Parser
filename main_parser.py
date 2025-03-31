import aiohttp
import asyncio
import json
from datetime import datetime
import logging
import re
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional
from functools import lru_cache
import random
import schedule


class BaseParser(ABC):
    def __init__(self, category: str, retries: int = 3, timeout: int = 30):
        self.category = category
        self.parsed_data = []
        self.logger = None
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.base_url = "https://orsha-ecokarta.gov.by/api"

    def _setup_logging(self, api_name: str) -> logging.Logger:
        logger = logging.getLogger(api_name)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            file_handler = logging.FileHandler(f"logs/{api_name}_parse.log", encoding="utf-8")
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        return logger

    async def fetch_data(self, api_name: str, api_url: str, session: aiohttp.ClientSession) -> List[Dict]:
        await asyncio.sleep(random.uniform(0.5, 2.0))
        attempts = 0
        max_attempts = self.retries
        while attempts < max_attempts:
            try:
                self.logger.info(f"Попытка {attempts + 1}/{max_attempts}. Отправляем запрос к API: {api_url}")
                async with session.get(api_url, headers={"User-Agent": "Mozilla/5.0"},
                                       timeout=self.timeout) as response:
                    response.raise_for_status()
                    self.logger.info(f"Запрос успешен. Статус: {response.status}")
                    self.logger.debug(f"Сырой ответ API: {await response.text()}")
                    data = await response.json()
                    if "nodes" not in data:
                        self.logger.error(f"Ключ 'nodes' отсутствует в ответе API. Структура ответа: {data}")
                        return []
                    return data
            except asyncio.TimeoutError as e:
                attempts += 1
                self.logger.warning(
                    f"Таймаут при запросе к API (попытка {attempts}/{max_attempts}): {api_url}, ошибка: {e}")
                if attempts == max_attempts:
                    self.logger.error(f"Достигнуто максимальное количество попыток. Ошибка: {e}")
                    return []
                await asyncio.sleep(2 ** attempts)
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    self.logger.error(f"API не найдено (404): {api_url}")
                else:
                    self.logger.error(f"Ошибка при запросе к API: {e}")
                return []
            except aiohttp.ClientError as e:
                self.logger.error(f"Ошибка при запросе к API: {e}")
                return []
        return []

    @staticmethod
    def extract_year(period_str: str) -> str:
        try:
            parts = period_str.split()
            if parts[-1].isdigit() and len(parts[-1]) == 4:
                return parts[-1]
            if re.match(r"\d{2}\.\d{2}\.\d{4}", period_str):
                return period_str.split(".")[-1]
            logging.getLogger().error(f"Не удалось извлечь год из: {period_str}")
            return "Unknown"
        except (IndexError, ValueError) as e:
            logging.getLogger().error(f"Ошибка при извлечении года: {period_str}, ошибка: {e}")
            return "Unknown"

    @staticmethod
    def parse_geolocation(geolocation_str: str) -> Tuple[Optional[float], Optional[float]]:
        try:
            if "Широта" in geolocation_str and "Долгота" in geolocation_str:
                parts = geolocation_str.split()
                lat = float(parts[1])
                lng = float(parts[3])
                logging.getLogger().debug(f"Успешно распарсены координаты: lat={lat}, lng={lng}")
                return lat, lng
            logging.getLogger().debug(f"Координаты не найдены в строке: {geolocation_str}")
            return None, None
        except (IndexError, ValueError) as e:
            logging.getLogger().error(f"Ошибка при парсинге координат: {geolocation_str}, ошибка: {e}")
            return None, None

    def save_data(self, api_name: str, data_type: str, initial_parse: bool = False) -> None:
        if not self.parsed_data:
            self.logger.info(f"Нет данных для сохранения для {api_name} ({data_type}). Пропускаем сохранение.")
            print(f"Нет данных для сохранения для {api_name} ({data_type}). Пропускаем сохранение.")
            return

        filename = f"{self.category}/{api_name}_{data_type}.json"
        existing_data = []

        if os.path.exists(filename) and not initial_parse:
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except Exception as e:
                self.logger.error(f"Ошибка при чтении файла {filename}: {e}")
                print(f"Ошибка при чтении файла {filename}: {e}")
                return

        if initial_parse:
            updated_data = self.parsed_data
        else:
            existing_nids = {item["Nid"] for item in existing_data}
            new_records = [item for item in self.parsed_data if item["Nid"] not in existing_nids]
            if not new_records:
                self.logger.info(f"Новых данных для {api_name} ({data_type}) не найдено.")
                print(f"Новых данных для {api_name} ({data_type}) не найдено.")
                return
            updated_data = existing_data + new_records
            self.logger.info(f"Найдено новых записей: {len(new_records)} для {api_name} ({data_type})")

        try:
            with open(filename, "w", encoding="utf-8", buffering=8192) as f:
                json.dump(updated_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Данные сохранены в файл: {filename} (всего записей: {len(updated_data)})")
            print(f"Данные сохранены в файл: {filename} (всего записей: {len(updated_data)})")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении данных в файл {filename}: {e}")
            print(f"Ошибка при сохранении данных в файл {filename}: {e}")

    def extract_geo_points(self, data_type: str = "place") -> set:
        """Извлекает все geo_point из файлов категории."""
        geo_points = set()

        if self.category == "water":
            # Для категории water извлекаем geo_point только из двух файлов
            target_files = [
                "nsmos_substances_substances.json",
                "esawage_surface_substances_substances.json"
            ]
            for filename in target_files:
                filepath = os.path.join(self.category, filename)
                if os.path.exists(filepath):
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            for item in data:
                                geo_point = item.get("field_geo_point", "")
                                if geo_point and geo_point != "Unknown":
                                    geo_points.add(geo_point)
                                    self.logger.info(f"Извлечён geo_point {geo_point} из файла {filename}")
                    except Exception as e:
                        self.logger.error(f"Ошибка при чтении файла {filepath}: {e}")
                else:
                    self.logger.warning(f"Файл {filepath} не найден")
        else:
            # Для остальных категорий извлекаем geo_point из всех файлов
            for filename in os.listdir(self.category):
                if filename.endswith(f"_{data_type}.json"):
                    filepath = os.path.join(self.category, filename)
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            for item in data:
                                geo_point = item.get("field_geo_point", item.get("Nid", ""))
                                if geo_point and geo_point != "Unknown":
                                    geo_points.add(geo_point)
                    except Exception as e:
                        self.logger.error(f"Ошибка при чтении файла {filepath}: {e}")

        self.logger.info(f"Всего уникальных field_geo_point для категории {self.category}: {len(geo_points)}")
        return geo_points

    async def parse_point_data(self, geo_point: str, session: aiohttp.ClientSession) -> dict:
        """Парсит данные из /api/sources_values для конкретного geo_point."""
        api_name = f"sources_values_{geo_point}"
        api_url = f"{self.base_url}/sources_values?geo_point={geo_point}"
        self.logger = self._setup_logging(api_name)
        data = await self.fetch_data(api_name, api_url, session)
        if not data or not data.get("nodes"):
            self.logger.info(f"Нет данных для geo_point {geo_point} в {api_url}")
            return {}

        # Обрабатываем поле php, если оно есть
        for item in data["nodes"]:
            node = item.get("node", {})
            php_data = node.get("php", "")
            if php_data:
                try:
                    substances = json.loads(php_data)
                    node["substances"] = substances
                    del node["php"]
                except json.JSONDecodeError as e:
                    self.logger.error(f"Ошибка при парсинге php для geo_point {geo_point}: {e}")
                    node["substances"] = []

        return data

    def save_point_data(self, geo_point: str, data: dict, initial_parse: bool = False) -> None:
        """Сохраняет данные точки в подпапку point_data."""
        if not data or not data.get("nodes"):
            self.logger.info(f"Нет данных для сохранения для geo_point {geo_point}. Пропускаем сохранение.")
            return

        # Создаём подпапку point_data
        point_data_dir = os.path.join(self.category, "point_data")
        os.makedirs(point_data_dir, exist_ok=True)

        filename = os.path.join(point_data_dir, f"point_{geo_point}.json")
        existing_data = []

        if os.path.exists(filename) and not initial_parse:
            try:
                with open(filename, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except Exception as e:
                self.logger.error(f"Ошибка при чтении файла {filename}: {e}")
                return

        if initial_parse:
            updated_data = data["nodes"]
        else:
            existing_periods = {item["node"]["field_period"] for item in existing_data}
            new_nodes = [item for item in data["nodes"] if item["node"]["field_period"] not in existing_periods]
            if not new_nodes:
                self.logger.info(f"Новых данных для geo_point {geo_point} не найдено.")
                return
            updated_data = existing_data + new_nodes
            self.logger.info(f"Найдено новых записей: {len(new_nodes)} для geo_point {geo_point}")

        try:
            with open(filename, "w", encoding="utf-8", buffering=8192) as f:
                json.dump(updated_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Данные сохранены в файл: {filename} (всего записей: {len(updated_data)})")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении данных в файл {filename}: {e}")

    @abstractmethod
    async def parse(self, session: aiohttp.ClientSession, initial_parse: bool = False) -> None:
        pass

    @abstractmethod
    def print_data(self) -> None:
        pass


class AirParser(BaseParser):
    def __init__(self, retries: int = 3, timeout: int = 30):
        super().__init__("air", retries, timeout)
        self.endpoints = [
            ("ala_place", "https://orsha-ecokarta.gov.by/api/ala_place"),
            ("lca_place", "https://orsha-ecokarta.gov.by/api/lca_place"),
            ("monitoring_aa_orsha", "https://orsha-ecokarta.gov.by/api/monitoring_aa_orsha"),
            ("monitoring_aas_orsha", "https://orsha-ecokarta.gov.by/api/monitoring_aas_orsha"),
            ("orsha-ap-substances", "https://orsha-ecokarta.gov.by/api/orsha-ap-substances"),
            ("saemission-orsha", "https://orsha-ecokarta.gov.by/api/saemission-orsha"),
        ]

    async def _parse_endpoint(self, endpoint: str, api_url: str, session: aiohttp.ClientSession) -> List[Dict]:
        self.logger = self._setup_logging(f"air_{endpoint}")
        nodes = await self.fetch_data(endpoint, api_url, session)
        if not nodes:
            self.logger.error(f"Не удалось извлечь данные для {endpoint}.")
            return []

        missing_php_count = 0
        missing_units_count = 0
        total_records = len(nodes["nodes"])
        parsed_data = []

        for item in nodes["nodes"]:
            node = item.get("node", {})
            self.logger.debug(f"Обрабатываем элемент node: {node}")
            parsed_item = {
                "title": node.get("title", "Unknown").strip(),
                "Nid": node.get("Nid", "Unknown"),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            if endpoint in ["ala_place", "lca_place", "saemission-orsha", "monitoring_aa_orsha"]:
                parsed_item["field_point_id"] = node.get("field_point_id", "Unknown")
                geolocation_str = node.get("field_geolocation", "")
                if geolocation_str:
                    lat, lng = self.parse_geolocation(geolocation_str)
                    parsed_item["lat"] = lat
                    parsed_item["lng"] = lng
                if endpoint in ["ala_place", "lca_place", "monitoring_aa_orsha"]:
                    parsed_item["nothing"] = node.get("nothing", "Unknown")

            elif endpoint == "monitoring_aas_orsha":
                parsed_item["field_geo_point"] = node.get("field_geo_point", "Unknown")
                parsed_item["field_source"] = node.get("field_source", "Unknown")
                parsed_item["field_period"] = node.get("field_period", "Unknown")
                parsed_item["title_1"] = node.get("title_1", "Unknown")
                parsed_item["title_2"] = node.get("title_2", "Unknown")

                php_data = node.get("php", "")
                self.logger.debug(f"Поле php для элемента {parsed_item['title']}: {php_data}")
                if php_data and php_data != "":
                    try:
                        php_parsed = json.loads(php_data)
                        self.logger.debug(f"Распарсенное php: {php_parsed}, тип: {type(php_parsed)}")
                        php_dict = {}
                        if isinstance(php_parsed, list):
                            if php_parsed:
                                php_dict = php_parsed[0]
                                if not isinstance(php_dict, dict):
                                    php_dict = {}
                                    self.logger.warning(
                                        f"Первый элемент php не является словарем для элемента {parsed_item['title']}: {php_dict}")
                            else:
                                self.logger.info(f"php является пустым списком для элемента {parsed_item['title']}")
                        elif isinstance(php_parsed, dict):
                            php_dict = php_parsed
                        else:
                            self.logger.warning(
                                f"php имеет неожиданный тип для элемента {parsed_item['title']}: тип: {type(php_parsed)}, значение: {php_parsed}")

                        if "field_units_reference" in php_dict:
                            parsed_item["field_units_reference"] = php_dict.get("field_units_reference", "Unknown")
                        elif "field_units_referencee" in php_dict:
                            parsed_item["field_units_reference"] = php_dict.get("field_units_referencee", "Unknown")
                        else:
                            parsed_item["field_units_reference"] = "Unknown"
                            missing_units_count += 1
                            self.logger.info(
                                f"field_units_reference и field_units_referencee не найдены в php для элемента {parsed_item['title']}: {php_dict}")

                        parsed_item["field_units_reference_id"] = php_dict.get("field_units_reference_id",
                                                                               php_dict.get("field_units_referencee_id",
                                                                                            "Unknown"))
                        parsed_item["field_pdk"] = php_dict.get("field_pdk", "Unknown")
                        parsed_item["field_pollutant"] = php_dict.get("field_pollutant", "Unknown")
                        parsed_item["field_pollutant_id"] = php_dict.get("field_pollutant_id", "Unknown")
                        parsed_item["field_average_concentration"] = php_dict.get("field_average_concentration",
                                                                                  "Unknown")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Ошибка при парсинге php для элемента {parsed_item['title']}: {e}")
                        parsed_item["field_units_reference"] = "Unknown"
                        parsed_item["field_units_reference_id"] = "Unknown"
                        parsed_item["field_pdk"] = "Unknown"
                        parsed_item["field_pollutant"] = "Unknown"
                        parsed_item["field_pollutant_id"] = "Unknown"
                        parsed_item["field_average_concentration"] = "Unknown"
                        missing_units_count += 1
                else:
                    parsed_item["field_units_reference"] = "Unknown"
                    parsed_item["field_units_reference_id"] = "Unknown"
                    parsed_item["field_pdk"] = "Unknown"
                    parsed_item["field_pollutant"] = "Unknown"
                    parsed_item["field_pollutant_id"] = "Unknown"
                    parsed_item["field_average_concentration"] = "Unknown"
                    missing_php_count += 1
                    missing_units_count += 1
                    self.logger.info(f"Поле php отсутствует или пустое для элемента {parsed_item['title']}")

            elif endpoint == "orsha-ap-substances":
                parsed_item["field_geo_point"] = node.get("field_geo_point", "Unknown")
                parsed_item["field_source"] = node.get("field_source", "Unknown")
                parsed_item["field_period"] = node.get("field_period", "Unknown")
                parsed_item["title_1"] = node.get("title_1", "Unknown")
                parsed_item["title_2"] = node.get("title_2", "Unknown")

                substances = []
                php_data = node.get("php", "")
                self.logger.debug(f"Поле php для элемента {parsed_item['title']}: {php_data}")
                if php_data and php_data != "Unknown":
                    try:
                        substances_list = json.loads(php_data)
                        self.logger.debug(
                            f"Распарсенное php для substances: {substances_list}, тип: {type(substances_list)}")
                        if isinstance(substances_list, list):
                            for substance in substances_list:
                                substance_data = {
                                    "field_name_of_substance": substance.get("field_name_of_substance", "Unknown"),
                                    "field_name_of_substance_id": substance.get("field_name_of_substance_id",
                                                                                "Unknown"),
                                    "field_code": substance.get("field_code", "Unknown"),
                                    "field_danger_class": substance.get("field_danger_class", "Unknown"),
                                    "field_znachenie": substance.get("field_znachenie", "Unknown"),
                                    "field_year_value": substance.get("field_year_value", "Unknown")
                                }
                                substances.append(substance_data)
                        else:
                            self.logger.warning(f"php для {endpoint} не является списком: {substances_list}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Ошибка при парсинге php для элемента {parsed_item['title']}: {e}")
                parsed_item["substances"] = substances

            parsed_data.append(parsed_item)
            self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        if endpoint == "monitoring_aas_orsha":
            self.logger.info(
                f"Статистика для {endpoint}: {missing_php_count} из {total_records} записей не содержат поле php")
            self.logger.info(
                f"Статистика для {endpoint}: {missing_units_count} из {total_records} записей не содержат field_units_reference")

        return parsed_data

    async def parse(self, session: aiohttp.ClientSession, initial_parse: bool = False) -> None:
        # Сначала парсим основные данные (это нужно для сохранения исходных файлов)
        tasks = [self._parse_endpoint(endpoint, api_url, session) for endpoint, api_url in self.endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for endpoint, result in zip([endpoint for endpoint, _ in self.endpoints], results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка при обработке эндпоинта {endpoint}: {result}")
                continue
            self.parsed_data = result
            self.print_data()
            self.save_data(endpoint, "data", initial_parse)

        # Для air не делаем запросы к API, а просто разделяем данные из orsha-ap-substances_data.json
        orsha_ap_file = "orsha-ap-substances_data.json"
        filepath = os.path.join(self.category, orsha_ap_file)
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Группируем данные по field_geo_point
                    geo_point_data = {}
                    for item in data:
                        geo_point = item.get("field_geo_point", "")
                        if geo_point and geo_point != "Unknown":
                            if geo_point not in geo_point_data:
                                geo_point_data[geo_point] = []
                            geo_point_data[geo_point].append(item)
                            self.logger.info(f"Добавлена запись для geo_point {geo_point} из файла {orsha_ap_file}")

                    # Сохраняем данные в отдельные файлы
                    for geo_point, items in geo_point_data.items():
                        self.save_point_data(geo_point, {"nodes": [{"node": item} for item in items]}, initial_parse)
                    self.logger.info(f"Сохранено {len(geo_point_data)} файлов в {self.category}/point_data")
            except Exception as e:
                self.logger.error(f"Ошибка при чтении файла {filepath}: {e}")
        else:
            self.logger.error(f"Файл {filepath} не найден")

    def print_data(self) -> None:
        pass


class GroundwaterParser(BaseParser):
    def __init__(self, retries: int = 3, timeout: int = 30):
        super().__init__("groundwater", retries, timeout)
        self.api_endpoints = [
            ("lgm_water", "https://orsha-ecokarta.gov.by/api/lgm_water"),
            ("lgm_water_subs", "https://orsha-ecokarta.gov.by/api/lgm_water_substances"),
            ("sg_water", "https://orsha-ecokarta.gov.by/api/sg_water"),
            ("sg_water_subst", "https://orsha-ecokarta.gov.by/api/sg_water_substances"),
        ]
        self.substances_apis = ["lgm_water_subs", "sg_water_subst"]
        self.place_apis = ["lgm_water", "sg_water"]

    async def _parse_endpoint(self, api_name: str, api_url: str, session: aiohttp.ClientSession) -> List[Dict]:
        self.logger = self._setup_logging(api_name)
        parsed_data = []

        if api_name in self.substances_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_geo_point": node.get("field_geo_point", "Unknown"),
                    "title_1": node.get("title_1", "Unknown"),
                    "field_source": node.get("field_source", "Unknown"),
                    "title_2": node.get("title_2", "Unknown"),
                    "field_type_of_mean": node.get("field_type_of_mean", "Unknown"),
                    "field_period": node.get("field_period", "Unknown"),
                    "year": self.extract_year(node.get("field_period", "Unknown")),
                    "field_index_vods_reg": node.get("field_index_vods_reg", "Unknown"),
                    "field_ob_nab": node.get("field_ob_nab", "Unknown"),
                    "field_number_skv": node.get("field_number_skv", "Unknown"),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                php_data = node.get("php", "")
                substances = []
                if php_data and php_data != "":
                    try:
                        php_parsed = json.loads(php_data)
                        self.logger.debug(f"Поле php для элемента {parsed_item['title']}: {php_data}")
                        if isinstance(php_parsed, list):
                            for substance in php_parsed:
                                substance_data = {
                                    "name": substance.get("field_name_of_substance",
                                                          substance.get("field_nab_pok", "Unknown")),
                                    "substance_id": substance.get("field_name_of_substance_id",
                                                                  substance.get("field_nab_pok_id", "Unknown")),
                                    "units": substance.get("field_units_referencee", "Unknown"),
                                    "units_id": substance.get("field_units_referencee_id", "Unknown")
                                }
                                if api_name == "lgm_water_subs":
                                    substance_data["number_skv"] = substance.get("field_number_skv", "Unknown")
                                    substance_data["value"] = substance.get("field_fact_conct", "Unknown")
                                elif api_name == "sg_water_subst":
                                    substance_data["value"] = substance.get("field_konc_vesh", "Unknown")
                                    substance_data["norm"] = substance.get("field_norm_zn", "Unknown")
                                substances.append(substance_data)
                        else:
                            self.logger.warning(
                                f"php не является списком для элемента {parsed_item['title']}: {php_parsed}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Ошибка при парсинге php для элемента {parsed_item['title']}: {e}")
                else:
                    self.logger.info(f"Поле php отсутствует или пустое для элемента {parsed_item['title']}")

                parsed_item["substances"] = substances
                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        elif api_name in self.place_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_adress": node.get("field_adress", "Unknown"),
                    "field_point_id": node.get("field_point_id", "Unknown"),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                geolocation_str = node.get("field_geolocation", "")
                lat, lng = self.parse_geolocation(geolocation_str)
                parsed_item["lat"] = lat
                parsed_item["lng"] = lng

                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        return parsed_data

    async def parse(self, session: aiohttp.ClientSession, initial_parse: bool = False) -> None:
        # Сначала парсим основные данные
        tasks = [
            self._parse_endpoint(api_name, api_url, session)
            for api_name, api_url in self.api_endpoints
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (api_name, _), result in zip(self.api_endpoints, results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка при обработке API {api_name}: {result}")
                continue
            self.parsed_data = result
            self.print_data()
            data_type = "substances" if api_name in self.substances_apis else "place"
            self.save_data(api_name, data_type, initial_parse)

        # Парсим данные для каждой точки
        geo_points = self.extract_geo_points("place") | self.extract_geo_points("substances")
        self.logger.info(f"Извлечено {len(geo_points)} geo_point для категории {self.category}")
        for geo_point in geo_points:
            point_data = await self.parse_point_data(geo_point, session)
            self.save_point_data(geo_point, point_data, initial_parse)

    def print_data(self) -> None:
        pass


class RadiationParser(BaseParser):
    def __init__(self, retries: int = 3, timeout: int = 30):
        super().__init__("radiation", retries, timeout)
        self.api_endpoints = [
            ("radiation_p_orsha", "https://orsha-ecokarta.gov.by/api/radiation_p_orsha"),
            ("radiation_p_orsha_s", "https://orsha-ecokarta.gov.by/api/radiation_p_orsha_s"),
        ]
        self.levels_apis = ["radiation_p_orsha_s"]
        self.place_apis = ["radiation_p_orsha"]

    async def _parse_endpoint(self, api_name: str, api_url: str, session: aiohttp.ClientSession) -> List[Dict]:
        self.logger = self._setup_logging(api_name)
        parsed_data = []

        if api_name in self.levels_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_geo_point": node.get("field_geo_point", "Unknown"),
                    "title_1": node.get("title_1", "Unknown"),
                    "field_source": node.get("field_source", "Unknown"),
                    "title_2": node.get("title_2", "Unknown"),
                    "field_period": node.get("field_period", "Unknown"),
                    "year": self.extract_year(node.get("field_period", "Unknown")),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                php_data = node.get("php", "")
                radiation_levels = []
                if php_data and php_data != "":
                    try:
                        php_parsed = json.loads(php_data)
                        self.logger.debug(f"Поле php для элемента {parsed_item['title']}: {php_data}")
                        if isinstance(php_parsed, list):
                            for level in php_parsed:
                                level_data = {
                                    "number": level.get("field_number_n", "Unknown"),
                                    "level": level.get("field_yroven", "Unknown"),
                                    "location": level.get("field_rasp_p", "Unknown"),
                                    "name": level.get("field_naimenovanie", "Unknown")
                                }
                                radiation_levels.append(level_data)
                        else:
                            self.logger.warning(
                                f"php не является списком для элемента {parsed_item['title']}: {php_parsed}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Ошибка при парсинге php для элемента {parsed_item['title']}: {e}")
                else:
                    self.logger.info(f"Поле php отсутствует или пустое для элемента {parsed_item['title']}")

                parsed_item["radiation_levels"] = radiation_levels
                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        elif api_name in self.place_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_adress": node.get("field_adress", "Unknown"),
                    "field_point_id": node.get("field_point_id", "Unknown"),
                    "nothing": node.get("nothing", "Unknown"),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                geolocation_str = node.get("field_geolocation", "")
                lat, lng = self.parse_geolocation(geolocation_str)
                parsed_item["lat"] = lat
                parsed_item["lng"] = lng

                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        return parsed_data

    async def parse(self, session: aiohttp.ClientSession, initial_parse: bool = False) -> None:
        # Сначала парсим основные данные
        tasks = [
            self._parse_endpoint(api_name, api_url, session)
            for api_name, api_url in self.api_endpoints
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (api_name, _), result in zip(self.api_endpoints, results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка при обработке API {api_name}: {result}")
                continue
            self.parsed_data = result
            self.print_data()
            data_type = "levels" if api_name in self.levels_apis else "place"
            self.save_data(api_name, data_type, initial_parse)

        # Парсим данные для каждой точки
        geo_points = self.extract_geo_points("place") | self.extract_geo_points("levels")
        self.logger.info(f"Извлечено {len(geo_points)} geo_point для категории {self.category}")
        for geo_point in geo_points:
            point_data = await self.parse_point_data(geo_point, session)
            self.save_point_data(geo_point, point_data, initial_parse)

    def print_data(self) -> None:
        pass


class SoilsParser(BaseParser):
    def __init__(self, retries: int = 3, timeout: int = 30):
        super().__init__("soils", retries, timeout)
        self.api_endpoints = [
            ("mcclb_territories", "https://orsha-ecokarta.gov.by/api/mcclb_territories"),
            ("mcclb_territories_substances", "https://orsha-ecokarta.gov.by/api/mcclb_territories_substances"),
            ("mchcl_settlements", "https://orsha-ecokarta.gov.by/api/mchcl-settlements"),
            ("mchcl_settlements_substances", "https://orsha-ecokarta.gov.by/api/mchcl-settlements-substances"),
            ("ground_local_monitoring", "https://orsha-ecokarta.gov.by/api/ground-local-monitoring"),
            ("ground_local_monitoring_substances",
             "https://orsha-ecokarta.gov.by/api/ground-local-monitoring-substances"),
        ]
        self.substances_apis = ["mcclb_territories_substances", "mchcl_settlements_substances",
                                "ground_local_monitoring_substances"]
        self.place_apis = ["mcclb_territories", "mchcl_settlements", "ground_local_monitoring"]

    async def _parse_endpoint(self, api_name: str, api_url: str, session: aiohttp.ClientSession) -> List[Dict]:
        self.logger = self._setup_logging(api_name)
        parsed_data = []

        if api_name in self.substances_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_geo_point": node.get("field_geo_point", "Unknown"),
                    "title_1": node.get("title_1", "Unknown"),
                    "field_source": node.get("field_source", "Unknown"),
                    "title_2": node.get("title_2", "Unknown"),
                    "field_period": node.get("field_period", "Unknown"),
                    "year": self.extract_year(node.get("field_period", "Unknown")),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                php_data = node.get("php", "")
                substances = []
                if php_data and php_data != "":
                    try:
                        php_parsed = json.loads(php_data)
                        self.logger.debug(f"Поле php для элемента {parsed_item['title']}: {php_data}")
                        if isinstance(php_parsed, list):
                            for substance in php_parsed:
                                substance_data = {
                                    "name": substance.get("field_name_of_substance", "Unknown"),
                                    "substance_id": substance.get("field_name_of_substance_id", "Unknown"),
                                    "units": substance.get("field_units_referencee", "Unknown"),
                                    "units_id": substance.get("field_units_referencee_id", "Unknown")
                                }
                                if api_name in ["mcclb_territories_substances", "mchcl_settlements_substances"]:
                                    substance_data["pdk_odk"] = substance.get("field_pdk_odk", "Unknown")
                                    substance_data["soderz"] = substance.get("field_soderz", "Unknown")
                                elif api_name == "ground_local_monitoring_substances":
                                    substance_data["actual_value"] = substance.get("field_actul_value", "Unknown")
                                    substance_data["pdk"] = substance.get("field_pdk", "Unknown")
                                    substance_data["quarterly_average"] = substance.get("field_quarterly_average",
                                                                                        "Unknown")
                                    substance_data["local_analytical_average"] = substance.get(
                                        "field_local_analytical_avarage", "Unknown")
                                    substance_data["analytical_actual_value"] = substance.get(
                                        "field_analytical_actual_value", "Unknown")
                                substances.append(substance_data)
                        else:
                            self.logger.warning(
                                f"php не является списком для элемента {parsed_item['title']}: {php_parsed}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Ошибка при парсинге php для элемента {parsed_item['title']}: {e}")
                else:
                    self.logger.info(f"Поле php отсутствует или пустое для элемента {parsed_item['title']}")

                parsed_item["substances"] = substances
                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        elif api_name in self.place_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_adress": node.get("field_adress", "Unknown"),
                    "field_point_id": node.get("field_point_id", "Unknown"),
                    "field_fn_isp": node.get("field_fn_isp", "Unknown"),
                    "field_ground_ch": node.get("field_ground_ch", "Unknown"),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                geolocation_str = node.get("field_geolocation", "")
                lat, lng = self.parse_geolocation(geolocation_str)
                parsed_item["lat"] = lat
                parsed_item["lng"] = lng

                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        return parsed_data

    async def parse(self, session: aiohttp.ClientSession, initial_parse: bool = False) -> None:
        # Сначала парсим основные данные
        tasks = [
            self._parse_endpoint(api_name, api_url, session)
            for api_name, api_url in self.api_endpoints
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (api_name, _), result in zip(self.api_endpoints, results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка при обработке API {api_name}: {result}")
                continue
            self.parsed_data = result
            self.print_data()
            data_type = "substances" if api_name in self.substances_apis else "place"
            self.save_data(api_name, data_type, initial_parse)

        # Парсим данные для каждой точки
        geo_points = self.extract_geo_points("place") | self.extract_geo_points("substances")
        self.logger.info(f"Извлечено {len(geo_points)} geo_point для категории {self.category}")
        for geo_point in geo_points:
            point_data = await self.parse_point_data(geo_point, session)
            self.save_point_data(geo_point, point_data, initial_parse)

    def print_data(self) -> None:
        pass


class WaterParser(BaseParser):
    def __init__(self, retries: int = 3, timeout: int = 30):
        super().__init__("water", retries, timeout)
        self.api_endpoints = [
            ("nsmos_substances", "https://orsha-ecokarta.gov.by/api/nsmos_substances"),
            ("surface_lm_substances", "https://orsha-ecokarta.gov.by/api/surface_lm_substances"),
            ("esawage_surface_substances", "https://orsha-ecokarta.gov.by/api/esawage_surface_substances"),
            ("surface_lm_place", "https://orsha-ecokarta.gov.by/api/surface_lm_place"),
            ("nsmos_place", "https://orsha-ecokarta.gov.by/api/nsmos_place"),
            ("esawage_surface_place", "https://orsha-ecokarta.gov.by/api/esawage_surface_place"),
            ("analit_control_s_and_p", "https://orsha-ecokarta.gov.by/api/analit_control_s_and_p"),
        ]
        self.substances_apis = ["nsmos_substances", "surface_lm_substances", "esawage_surface_substances"]
        self.place_apis = ["surface_lm_place", "nsmos_place", "esawage_surface_place", "analit_control_s_and_p"]

    async def _parse_endpoint(self, api_name: str, api_url: str, session: aiohttp.ClientSession) -> List[Dict]:
        self.logger = self._setup_logging(api_name)
        parsed_data = []

        if api_name in self.substances_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_geo_point": node.get("field_geo_point", "Unknown"),
                    "title_1": node.get("title_1", "Unknown"),
                    "field_source": node.get("field_source", "Unknown"),
                    "title_2": node.get("title_2", "Unknown"),
                    "field_type_of_mean": node.get("field_type_of_mean", "Unknown"),
                    "field_period": node.get("field_period", "Unknown"),
                    "year": self.extract_year(node.get("field_period", "Unknown")),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                php_data = node.get("php", "")
                substances = []
                if php_data and php_data != "":
                    try:
                        php_parsed = json.loads(php_data)
                        self.logger.debug(f"Поле php для элемента {parsed_item['title']}: {php_data}")
                        if isinstance(php_parsed, list):
                            for substance in php_parsed:
                                substance_data = {
                                    "name": substance.get("field_name_of_substance", "Unknown"),
                                    "substance_id": substance.get("field_name_of_substance_id", "Unknown"),
                                    "units": substance.get("field_units_referencee", "Unknown"),
                                    "units_id": substance.get("field_units_referencee_id", "Unknown")
                                }
                                if api_name == "nsmos_substances":
                                    substance_data["period"] = substance.get("field_period", "Unknown")
                                    substance_data["value"] = substance.get("field_value", "Unknown")
                                else:
                                    substance_data["average_concentration_1"] = substance.get(
                                        "field_average_concentration_1", "Unknown")
                                    substance_data["average_concentration_2"] = substance.get(
                                        "field_average_concentration_2", "Unknown")
                                    substance_data["average_concentration_3"] = substance.get(
                                        "field_average_concentration_3", "Unknown")
                                substance_data["pdk"] = substance.get("field_pdk_text", "Unknown")
                                if api_name == "surface_lm_substances":
                                    substance_data["ds"] = substance.get("field_ds", "Unknown")
                                    substance_data["pdk_2"] = substance.get("field_pdk_text_2", "Unknown")
                                substances.append(substance_data)
                        else:
                            self.logger.warning(
                                f"php не является списком для элемента {parsed_item['title']}: {php_parsed}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Ошибка при парсинге php для элемента {parsed_item['title']}: {e}")
                else:
                    self.logger.info(f"Поле php отсутствует или пустое для элемента {parsed_item['title']}")

                parsed_item["substances"] = substances
                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        elif api_name in self.place_apis:
            nodes = await self.fetch_data(api_name, api_url, session)
            if not nodes:
                self.logger.error(f"Не удалось извлечь данные из API {api_name}.")
                return parsed_data

            for item in nodes["nodes"]:
                node = item.get("node", {})
                self.logger.debug(f"Обрабатываем элемент node: {node}")
                parsed_item = {
                    "title": node.get("title", "Unknown").strip(),
                    "Nid": node.get("Nid", "Unknown"),
                    "field_adress": node.get("field_adress", "Unknown"),
                    "field_point_id": node.get("field_point_id", "Unknown"),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                geolocation_str = node.get("field_geolocation", "")
                lat, lng = self.parse_geolocation(geolocation_str)
                parsed_item["lat"] = lat
                parsed_item["lng"] = lng

                parsed_data.append(parsed_item)
                self.logger.info(f"Успешно обработан элемент: {parsed_item}")

        return parsed_data

    async def parse(self, session: aiohttp.ClientSession, initial_parse: bool = False) -> None:
        # Сначала парсим основные данные
        tasks = [
            self._parse_endpoint(api_name, api_url, session)
            for api_name, api_url in self.api_endpoints
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (api_name, _), result in zip(self.api_endpoints, results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка при обработке API {api_name}: {result}")
                continue
            self.parsed_data = result
            self.print_data()
            data_type = "substances" if api_name in self.substances_apis else "place"
            self.save_data(api_name, data_type, initial_parse)

        # Парсим данные для каждой точки
        geo_points = self.extract_geo_points("place") | self.extract_geo_points("substances")
        self.logger.info(f"Извлечено {len(geo_points)} geo_point для категории {self.category}")
        for geo_point in geo_points:
            point_data = await self.parse_point_data(geo_point, session)
            self.save_point_data(geo_point, point_data, initial_parse)

    def print_data(self) -> None:
        pass


class ParserManager:
    def __init__(self):
        data_folders = ["air", "groundwater", "radiation", "soils", "water", "logs"]
        for folder in data_folders:
            os.makedirs(folder, exist_ok=True)

        self.parsers = [
            AirParser(),
            GroundwaterParser(),
            RadiationParser(),
            SoilsParser(),
            WaterParser(),
        ]
        self.loop = asyncio.new_event_loop()

    async def run_parse(self, initial_parse: bool = False) -> None:
        start_time = datetime.now()
        async with aiohttp.ClientSession() as session:
            tasks = [parser.parse(session, initial_parse) for parser in self.parsers]
            await asyncio.gather(*tasks)
        end_time = datetime.now()
        print(f"Парсинг завершён. Время выполнения: {(end_time - start_time).total_seconds():.2f} секунд")

    async def run_schedule(self):
        print("Изначальный парсинг всех данных...")
        await self.run_parse(initial_parse=True)
        print("Запуск периодического парсинга (каждый час)...")
        schedule.every(1).hours.do(lambda: self.loop.create_task(self.run_parse(initial_parse=False)))
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)

    def run(self) -> None:
        try:
            self.loop.run_until_complete(self.run_schedule())
        except KeyboardInterrupt:
            print("Программа остановлена пользователем.")

            tasks = [task for task in asyncio.all_tasks(self.loop) if task is not asyncio.current_task(self.loop)]
            for task in tasks:
                task.cancel()

            self.loop.stop()
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
            exit(0)


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    print("Программа запущена")
    manager = ParserManager()
    manager.run()