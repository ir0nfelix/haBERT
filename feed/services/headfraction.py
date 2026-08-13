import os
import sys

import duckdb

from feed.helpers import get_data_path
from feed.settings import (
    HEADFRACTION_HTTP_STATUSES_FILE,
    HEADFRACTION_HUBS_FREQUENCY_FILE,
    HEADFRACTION_POST_TYPES_FILE,
    HEADFRACTION_SEO_FLAGS_FILE,
    HEADFRACTION_TAG_EDGES_FILE,
    HEADFRACTION_TAGS_FREQUENCY_FILE,
    RAW_SCRAPES_FILE,
    settings,
)


def headfraction(input_file: str) -> None:
    if not os.path.exists(input_file):
        print(f"Ошибка: Файл {input_file} не найден.")
        return

    output_dir = os.path.dirname(os.path.abspath(input_file))
    http_statuses_path = get_data_path(HEADFRACTION_HTTP_STATUSES_FILE, output_dir)
    seo_flags_path = get_data_path(HEADFRACTION_SEO_FLAGS_FILE, output_dir)
    post_types_path = get_data_path(HEADFRACTION_POST_TYPES_FILE, output_dir)
    hubs_freq_path = get_data_path(HEADFRACTION_HUBS_FREQUENCY_FILE, output_dir)
    tags_freq_path = get_data_path(HEADFRACTION_TAGS_FREQUENCY_FILE, output_dir)
    tag_edges_path = get_data_path(HEADFRACTION_TAG_EDGES_FILE, output_dir)

    conn = duckdb.connect(":memory:")
    # ==========================================
    # ВЫБОРКА 1: ГЛОБАЛЬНАЯ ДЕДУПЛИКАЦИЯ
    # ==========================================
    conn.execute(f"""
        CREATE OR REPLACE VIEW unique_records AS
        SELECT *
        FROM read_json_auto('{input_file}')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY pub_id ORDER BY timestamp DESC) = 1;
    """)

    print("АНАЛИТИКА 1: Экспорт статистики по статусам")
    conn.execute(f"""
        COPY (
            SELECT 
                http_status, 
                COUNT(*) AS total_count 
            FROM unique_records 
            GROUP BY http_status 
            ORDER BY total_count DESC
        ) TO '{http_statuses_path}' (HEADER TRUE, DELIMITER ';');
    """)

    # ==========================================
    # ВЫБОРКА 2: СРЕЗ ПОЛЕЗНЫХ ДАННЫХ (типа CTE)
    # ==========================================
    conn.execute("""
        CREATE OR REPLACE VIEW valid_articles AS
        SELECT * 
        FROM unique_records 
        WHERE http_status = 200;
    """)

    # ==========================================
    # ВЫБОРКА 3: ФИЛЬТРАЦИЯ БИЗНЕС-МЕТРИК
    # ==========================================
    print("АНАЛИТИКА 2: Экспорт флагов - а ты точно SEO?")
    conn.execute(f"""
        COPY (
            SELECT is_seo, COUNT(*) AS count
            FROM valid_articles
            GROUP BY is_seo
            ORDER BY count DESC
        ) TO '{seo_flags_path}' (HEADER TRUE, DELIMITER ';');
    """)

    print("ВЫБОРКА 3: Экспорт типов постов (статья/новость)")
    conn.execute(f"""
        COPY (
            SELECT post_type, COUNT(*) AS count
            FROM valid_articles
            GROUP BY post_type
            ORDER BY count DESC
        ) TO '{post_types_path}' (HEADER TRUE, DELIMITER ';');
    """)

    print("ВЫБОРКА 4: Экспорт частотного словаря Хабов")
    conn.execute(f"""
        COPY (
            WITH unnested_data AS (
                SELECT unnest(hubs) AS hub_name
                FROM valid_articles
            )
            SELECT 
                hub_name, 
                COUNT(*) AS count
            FROM unnested_data
            GROUP BY hub_name
            ORDER BY count DESC
        ) TO '{hubs_freq_path}' (
            HEADER TRUE, 
            DELIMITER ';', 
            QUOTE '"', 
            FORCE_QUOTE (hub_name)
        );
    """)

    print("ВЫБОРКА 5: Экспорт частотного словаря Тэгов")
    conn.execute(f"""
        COPY (
            WITH unnested_data AS (
                SELECT unnest(tags) AS tag_name
                FROM valid_articles
            )
            SELECT 
                tag_name, 
                COUNT(*) AS count
            FROM unnested_data
            GROUP BY tag_name
            HAVING count > 5 
            ORDER BY count DESC
        ) TO '{tags_freq_path}' (
            HEADER TRUE, 
            DELIMITER ';', 
            QUOTE '"', 
            FORCE_QUOTE (tag_name)
        );
    """)

    print("ВЫБОРКА 6: Декартово произведение связей хаб-тэг")
    conn.execute(f"""
        COPY (
            SELECT 
                h.hub_name, 
                t.tag_name, 
                COUNT(*) AS frequency
            FROM valid_articles, 
                 unnest(hubs) AS h(hub_name), 
                 unnest(tags) AS t(tag_name)
            GROUP BY h.hub_name, t.tag_name
            ORDER BY frequency DESC
        ) TO '{tag_edges_path}' (
            HEADER FALSE,      
            DELIMITER ';',     
            QUOTE '"',         
            FORCE_QUOTE (hub_name, tag_name) 
        );
    """)
