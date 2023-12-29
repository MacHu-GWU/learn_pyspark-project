# -*- coding: utf-8 -*-

"""
This script scan all the jupyter notebook and build the full text search anything
alfred workflow dataset file at ``${HOME}/.alfred-afwf/aftf_fts_anything/``.
"""

import json
import re
from pathlib_mate import Path

domain = "https://learn-pyspark.readthedocs.io"

dir_project_root = Path.dir_here(__file__)
dir_home = Path.home()
dir_afwf_fts_anything = dir_home.joinpath(".alfred-afwf", "afwf_fts_anything")
dataset = "learnpyspark"
path_data = dir_afwf_fts_anything.joinpath(f"{dataset}-data.json")
path_setting = dir_afwf_fts_anything.joinpath(f"{dataset}-setting.json")

data = list()
pattern = re.compile("#+")
dir_source = dir_project_root.joinpath("docs", "source")
for path in dir_source.glob("**/*.ipynb"):
    if ".ipynb_checkpoints" in str(path):
        continue

    relpath = path.change(new_ext=".html").relative_to(dir_source)
    for cell in json.loads(path.read_text())["cells"]:
        if cell["cell_type"] == "markdown":
            for line in cell["source"]:
                flag = False
                if re.match(pattern, line):
                    if line.startswith("#"):
                        flag = True
                if flag:
                    header, title = line.strip().split(" ", 1)
                    level = len(header)
                    hash_tag = title.replace(" ", "-")
                    row = {
                        "title": title,
                        "header": header,
                        "weight": 10 - level,
                        "url": f"{domain}/{relpath}#{hash_tag}",
                    }
                    data.append(row)

settings = {
    "fields": [
        {
            "name": "title",
            "type_is_store": True,
            "type_is_ngram": True,
            "ngram_minsize": 2,
            "ngram_maxsize": 10,
        },
        {
            "name": "header",
            "type_is_store": True,
        },
        {
            "name": "weight",
            "type_is_store": True,
        },
        {
            "name": "url",
            "type_is_store": True,
        },
    ],
    "title_field": "{header} {title}",
    "subtitle_field": "open {url}",
    "arg_field": "{url}",
    "autocomplete_field": "{title}",
}
path_data.write_text(json.dumps(data, indent=4))
path_setting.write_text(json.dumps(settings, indent=4))
