import time
import subprocess
import pandas as pd
import os


today = time.localtime()
time_str = time.strftime("%m-%d-%YT%H:%M:%S.309Z", today)

# define time from when to start fetching LDES.#
fetch_from = "2022-02-16T00:00:00.309Z" ## change to 29 september 2021
context = "src/utils/context.jsonld"

endpoints = {
    # CLI commands to fetch LDES from actor-init-ldes-client
    "DMG": f"actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            + context + " --fromTime " + fetch_from + " --emitMemberOnce false --disablePolling true"
            f" https://apidg.gent.be/opendata/adlib2eventstream/v1/dmg/objecten",
    "HVA": "actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            + context + " --fromTime " + fetch_from + " --emitMemberOnce false --disablePolling true"
            " https://apidg.gent.be/opendata/adlib2eventstream/v1/hva/objecten",
    "STAM": "actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            + context + "  --fromTime " + fetch_from + " --emitMemberOnce false --disablePolling true"
            " https://apidg.gent.be/opendata/adlib2eventstream/v1/stam/objecten",
    "IM": "actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            + context + "   --fromTime " + fetch_from + " --emitMemberOnce false --disablePolling true"
            " https://apidg.gent.be/opendata/adlib2eventstream/v1/industriemuseum/objecten",
    "ARCH": "actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            + context + "   --fromTime  "+ fetch_from +" --emitMemberOnce false --disablePolling true"
            " https://apidg.gent.be/opendata/adlib2eventstream/v1/archiefgent/objecten",
    "THES": "actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            + context + "   --fromTime "+ fetch_from + " --emitMemberOnce false --disablePolling true"
            " https://apidg.gent.be/opendata/adlib2eventstream/v1/adlib/thesaurus",
    "AGENT": "actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context "
            +  context + "  --fromTime " + fetch_from + " --emitMemberOnce false --disablePolling true"
            " https://apidg.gent.be/opendata/adlib2eventstream/v1/adlib/personen"
}

ROOT_DIR = os.path.abspath(os.curdir)

filepath = {
    "DMG": ROOT_DIR + "\\data\\dmg_obj.json",
    "HVA": ROOT_DIR + "\\data\\hva_obj.json",
    "STAM": ROOT_DIR + "\\data\\stam_obj.json",
    "IM": ROOT_DIR + "\\data\\im_obj.json",
    "ARCH": ROOT_DIR + "\\data\\arch_obj.json",
    "THES": ROOT_DIR + "\\data\\thes.json",
    "AGENT": ROOT_DIR + "\\data\\agents.json"
}

# define columns to for dataframes
columns_obj = ["URI", "timestamp", "@type", "owner", "objectnumber", "title", "object_name", "object_name_id",
               "creator", "creator_role", "creation_date", "creation_place","provenance_date", "provenance_type",
               "material", "material_source", "description", "collection", "association", "location"]

columns_thes = ["URI", "timestamp", "term", "ext_URI"]

columns_agents = ["URI", "timestamp", "full_name", "family_name", "sirname", "name (organisations)", "date_of_birth",
                  "date_of_death", "place_of_birth", "place_of_death", "nationality", "gender","ulan", "wikidata","rkd", "same_as"]


def fetch_json(key):
    """read json from command line interface and write to .json file"""
    with open(filepath[key], "w") as f:
        if not os.path.exists(filepath[key]):
            file(filepath[key], 'w').close()
        p = subprocess.run(endpoints[key], shell=True, stdout=f, text=True)


def generate_dataframe(key):
    with open(filepath[key]) as p:
        res = p.read()
        res = res.splitlines()
        print("Done with parsing data from {}".format(key))
        # print("Done with parsing data from {}".format(key))
        return res

    fetch_json("DMG")
    generate_dataframe("DMG")
    df_dmg = pd.DataFrame(generate_dataframe("DMG"))
