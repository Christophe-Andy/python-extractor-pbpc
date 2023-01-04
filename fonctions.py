import pandas as pd
from data_format import *

def recup_nom_code_banque(df_x):
    # Code Banque et Nom de la banque 80001 - Banque X1
    code_nom_banque = df_x.iloc[3, 0]
    liste_code_nom_banque = code_nom_banque.split("-") 

    code_banque = liste_code_nom_banque[0]
    intitule_banque = liste_code_nom_banque[1]

    # Code Banque et Nom sans les espaces de debut et fin notamment

    intitule_banque = special_format(intitule_banque)
    code_banque = special_format(str(code_banque))

    # Ajout du code pays 

    code_pays = int(code_banque) - (int(code_banque) % (int(code_banque[:1]) * 10000))
    
    liste_insertion_nom_code_banque = [code_banque, intitule_banque, code_pays]

    return liste_insertion_nom_code_banque


def recup_nom_code_pays(df_x):
    # Code Banque et Nom de la banque 80001 - Banque X1
    code_nom_banque = df_x.iloc[3, 0]
    liste_code_nom_banque = code_nom_banque.split("-")

    # Code Banque
    code_banque = liste_code_nom_banque[0]
    code_banque = special_format(code_banque)

    # Nom Pays
    intitule_pays = df_x.iloc[4, 0]
    intitule_pays = special_format(intitule_pays)

    # Code Pays
    code_pays = int(code_banque) - (int(code_banque) % (int(code_banque[:1]) * 10000))

    liste_insertion_nom_code_pays = [code_pays, intitule_pays]

    return liste_insertion_nom_code_pays


# Recuperation de l'index de ligne ou commencent les libelles 

def recup_debut_data(df_x):
    debut_data = 5
    i = debut_data
    poste = ""
    
    while i < len(df_x) and (poste != "Libellé poste") and (poste != "Libellé") and (poste != "Intitulé") :
        poste = df_x.iloc[i,0]
        #debut data s'incremente en meme temps que le i avant la cellule de libelles
        debut_data += 1

        if (poste == "Libellé poste") or (poste == "Libellé") or (poste == "Intitulé")  :
            i += 1
            while (pd.isnull(df_x.iloc[i,0])):
                debut_data += 1
                print("Recherche de debut data...")
                i += 1
        i += 1
        

    return  debut_data


def recup_debut_entete(df_x):
    debut_entete = 5
    poste = df_x.iloc[debut_entete,0]
    
    while (poste != "Libellé poste") and (poste != "Libellé") and (poste != "Intitulé") :
        debut_entete += 1
        poste = df_x.iloc[debut_entete,0]
        
    return  debut_entete


def recup_postes_bilan(df_x):
    
    liste_insertions_postes_bilans = []

    debut_data = recup_debut_data(df_x)
    i = debut_data
    
    print("Boucle des postes(***)")
    
    for i in range(debut_data, len(df_x)):
        if not pd.isnull(df_x.iloc[i,0]):
            code_poste_bilan = df_x.iloc[i,1]
            intitule_poste_bilan = df_x.iloc[i,0]

            code_poste_bilan = special_format(str(code_poste_bilan))
            intitule_poste_bilan = special_format(intitule_poste_bilan)
        
            liste_insertion_poste_bilan = [code_poste_bilan, intitule_poste_bilan]
            print("***", liste_insertion_poste_bilan)
            liste_insertions_postes_bilans.append(liste_insertion_poste_bilan)
        elif pd.isnull(df_x.iloc[i,0]):
            break

    return  liste_insertions_postes_bilans


def gestion_cellules_fusionnees(df_x):
    #Remplacement des NaN par le nom de la colonne precedente
    debut_data = recup_debut_data(df_x)
    debut_entete = recup_debut_entete(df_x)
    
    for ligne in range (debut_entete, debut_data) :
        for colonne in range(2, len(df_x.columns)) :
            if pd.isnull(df_x.iloc[ligne,colonne]) and colonne < len(df_x.columns)-1:
                df_x.iat[ligne, colonne] = df_x.iloc[ligne, colonne-1]

    return df_x


def recup_colonnes_bilan(df_x):
    debut_data = recup_debut_data(df_x)
    debut_entete = recup_debut_entete(df_x)
    print("Debut des entetes : ", debut_entete)
    print("Debut des donnees brutes : ", debut_data)

    liste_intitules_colonnes_bilan = []

    for colonne in range(2,len(df_x.columns)):
        list_intitule_colonne = []
        
        # debut_entete = 6, 7 ou 8
        for ligne in range(debut_entete,debut_data) :
            entete_colonne = df_x.iloc[ligne,colonne]
            
            if not pd.isnull(entete_colonne) :
                entete_colonne = special_format(str(entete_colonne))
                list_intitule_colonne.append(entete_colonne)

                #Récupération de l’entete globale suivant un separateur " ; "
                intitule_colonne_bilan_sep =  ";".join(list_intitule_colonne)
                print("***",intitule_colonne_bilan_sep)

        #Recuperation de l’ensemble des codes colonnes entetes des colonnes de la feuille de DEC
        #liste_intitules_colonnes_bilan.append([code_clonne, intitule_colonne_bilan])
        code_colonne_bilan = special_format(list_intitule_colonne[-1]) + "_" + df_x.iloc[0,0]

        liste_intitules_colonnes_bilan.append([code_colonne_bilan, intitule_colonne_bilan_sep])
    
    print(liste_intitules_colonnes_bilan)

    return liste_intitules_colonnes_bilan


def affiche_dataframe_cells(new_df_x, debut, fin):
    for i in range(debut, fin):
        print("LIGNE ",i)
        for j in range(0, len(new_df_x.columns)):
            print(new_df_x.iloc[i,j])


def recup_mdp():
    return "admin1234"
