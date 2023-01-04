#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import oracledb
from fonctions import *
from data_format import *
from insertions import *

# Connexion a la base de donnees Oracle
try:
    # connexion = oracledb.connect("SYSTEM/DBst2022@localhost:1521/ORCLPROTODB")
    connexion = oracledb.connect(
        user="CHRISTOPHE",
        password=recup_mdp(),
        dsn="localhost:1521/ORCLPROTODB",
        # mode=oracledb.SYSDBA,
        encoding="UTF-8",
    )
    #print(type(connexion))
    print("Connexion a la base de donnees etablie !")

except:
    print("Echec de connexion !")


dec_name = "80001_2020_12_anonymous.xlsx"
df = file_to_dataframe(dec_name, None)
dec_list = list(df.keys())

print("Chargement du DEC : %s " % dec_name + " reussi !")
print("Nombre de feuilles chargees : %i" % len(dec_list))
print("Feuilles chargees : ", dec_list)
print("15 premieres lignes du %s : " % dec_list[0])

# Insertion de valeurs dans la table de la base de donnees
# df_x = df[dec_list[0]]  # DEC1001

list_dec_reduite_cal = ["DEC1001","DEC1002","DEC1003","DEC1007","DEC1008",
                    "DEC1009", "DEC1010","DEC1011","DEC1101","DEC1103",
                    "DEC1104","DEC1105", "DEC1110", "DEC1113","DEC1119",
                    "DEC1501","DEC1506"]
#list_dec_reduite = dec_list[0:28]
list_dec_reduite = ["DEC1001","DEC1002","DEC1003","DEC1004","DEC1005","DEC1006","DEC1007","DEC1008","DEC1009", "DEC1010",
                    "DEC1011","DEC1012","DEC1101","DEC1102","DEC1103","DEC1104","DEC1105", "DEC1106","DEC1107","DEC1108",
                    "DEC1109","DEC1110","DEC1111","DEC1112","DEC1113","DEC1114","DEC1115","DEC1116","DEC1118","DEC1119",
                    "DEC1123","DEC1138","DEC1139","DEC1501","DEC1502","DEC1503","DEC1504","DEC1505","DEC1506","DEC1507",
                    "DEC1508","DEC1509","DEC1510"]

print("Liste DEC utilises :", list_dec_reduite)
for nom_feuille_dec in list_dec_reduite:
    print(df[nom_feuille_dec].head(15))

nbr_insertions_ok = 0
liste_insertions_ok = []
liste_insertions_ko = []

for nom_feuille_dec in list_dec_reduite :
    df_x = df[nom_feuille_dec]
    insertion_log = insert_dataframe_to_oracle(connexion, df_x)
    nbr_insertions_ok += insertion_log[0]
    if insertion_log[0] == 1:
        liste_insertions_ok.append(insertion_log[1])
    else :
        liste_insertions_ko.append(insertion_log[1])

print("INSERTIONS EFFECTUEES : ",nbr_insertions_ok," / ",len(list_dec_reduite))
print("FEUILLES DEC IMPORTEES : ",liste_insertions_ok)
print("FEUILLES DEC RECALLEES: ",liste_insertions_ko)

if connexion:
   connexion.close()
