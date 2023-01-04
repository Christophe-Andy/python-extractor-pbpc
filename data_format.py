#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import json
from fonctions import *

def file_to_dataframe(path_x, sheet_x):
    df = pd.read_excel(path_x, sheet_name=sheet_x, header=None)
    return df


def excel_to_dataframe(path_x):
    df = pd.ExcelFile(path_x)
    return df


def recup_json_to_dict(path):
    with open(path) as entree_json:
        entree_dict = json.load(entree_json)

    return entree_dict


def special_format(intitule):
    # Gestion des espaces de debut et de fin de chaine
    intitule_final = intitule.strip()

    # Gestion des points-virgules
    intitule_final = intitule_final.replace(";", "-")

    # Gestion des egalites
    intitule_final = intitule_final.replace("=", "-")

    # Gestion SQL des apostrophes
    #intitule_final = intitule_final.replace("'", "''")
    #intitule_final = "q'[" + intitule_final + "]'"

    return intitule_final