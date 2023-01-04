import oracledb
from fonctions import *
from data_format import *

# Insertion de valeurs dans la table de la base de donnees

def insert_pays_to_oracle(cursor, liste_insertions_noms_codes_pays):
    
    # Test si le code poste du poste a inserer existe deja ou non dans la base de donnees 
    code_pays = str(liste_insertions_noms_codes_pays[0][0])
    cursor.execute("SELECT count(*) FROM PAYS WHERE CODE_PAYS = '"+code_pays+"'")
    existence = cursor.fetchall()

    print("\nTest d'existence Pays : ", existence[0][0])

    if  existence[0][0] == 0 :
        cursor.executemany(
                "INSERT into PAYS(CODE_PAYS, INTITULE_PAYS) values(:1,:2)",
                liste_insertions_noms_codes_pays,
            )
    
def insert_banque_to_oracle(cursor, liste_insertions_noms_codes_banques):
    
    code_banque = str(liste_insertions_noms_codes_banques[0][0])
   
    cursor.execute("SELECT count(*) FROM BANQUE WHERE CODE_BANQUE = '"+code_banque+"'")
    existence = cursor.fetchall()

    print("\nTest d'existence Banque : ", existence[0][0])

    if  existence[0][0] == 0 :
        cursor.executemany(
                "INSERT into BANQUE(CODE_BANQUE, INTITULE_BANQUE, CODE_PAYS) values(:1,:2,:3)",
                liste_insertions_noms_codes_banques,
            )

def insert_postes_to_oracle(cursor, liste_insertions_postes_bilan):
    
    for liste_insertion_poste_bilan in liste_insertions_postes_bilan :
        print("Poste a inserer : ", liste_insertion_poste_bilan)
        code_poste = str(liste_insertion_poste_bilan[0])
        
        cursor.execute("SELECT count(*) FROM POSTE_BILAN WHERE CODE_POSTE_BILAN = '"+code_poste+"'")
        existence = cursor.fetchall()

        print("\nTest d'existence Poste : ", existence[0][0])
        #print("Type existence : ", type(existence[0][0]))

        if  existence[0][0] == 0 :
            cursor.executemany(
                    "INSERT into POSTE_BILAN(CODE_POSTE_BILAN, INTITULE_POSTE_BILAN) values(:1,:2)",
                    [liste_insertion_poste_bilan]
                )
    
def insert_colonnes_to_oracle(cursor, liste_insertions_colonnes_bilan):
    for liste_insertion_colonne_bilan in liste_insertions_colonnes_bilan :
        print("Colonne a inserer : ", liste_insertion_colonne_bilan)
        
        #intitule_colonne = str(liste_insertion_colonne_bilan[1])
        code_colonne = str(liste_insertion_colonne_bilan[0])

        cursor.execute("SELECT count(*) FROM COLONNE_BILAN WHERE CODE_COLONNE_BILAN = '"+code_colonne+"'")
        print("SELECT count(*) FROM COLONNE_BILAN WHERE CODE_COLONNE_BILAN = '"+code_colonne+"'")
        existence = cursor.fetchall()

        print("\nTest d'existence Colonne : ", existence[0][0])
        #print("Type existence : ", type(existence[0][0]))

        if  existence[0][0] == 0 :
            #print("INSERT into COLONNE_BILAN(CODE_COLONNE_BILAN, INTITULE_COLONNE_BILAN) values(:1,:2)")
            print("Liste à ajouter : ",liste_insertion_colonne_bilan)
            cursor.executemany(
                    "INSERT into COLONNE_BILAN(CODE_COLONNE_BILAN, INTITULE_COLONNE_BILAN) values(:1,:2)",
                    [liste_insertion_colonne_bilan]
                )


def insert_dataframe_to_oracle(connexion, df_x):
    cursor = connexion.cursor()

    # Traitement de la partie superieure du DEC
    try:

        # Nom feuille de DEC
        nom_feuille_dec = df_x.iloc[0, 0]
        print("Nom feuille DEC : ", nom_feuille_dec)

        # Titre etat financier
        titre_etat_f = df_x.iloc[1, 0]
        print("Etat financier : ", titre_etat_f)

        # Date DEC
        date_dec = df_x.iloc[2, 0]
        print("Date : ", date_dec)

        liste_insertions_noms_codes_banques = []
        liste_insertions_noms_codes_pays = []

        print(recup_nom_code_banque(df_x))
        print(recup_nom_code_pays(df_x))

        liste_insertions_noms_codes_banques.append(recup_nom_code_banque(df_x))
        liste_insertions_noms_codes_pays.append(recup_nom_code_pays(df_x))
        liste_insertions_postes_bilan = recup_postes_bilan(df_x)

        print("Banque : ", liste_insertions_noms_codes_banques)
        print("Pays : ", liste_insertions_noms_codes_pays)
        print("Liste des postes : ", liste_insertions_postes_bilan)

        # Remplacement de None par les noms d'entetes correspondants
        new_df_x = gestion_cellules_fusionnees(df_x)
        
        nbr_lignes_affiche = 15
        if nbr_lignes_affiche > len(new_df_x):
           nbr_lignes_affiche = len(new_df_x)-1
        
        print(new_df_x.head(nbr_lignes_affiche))
        
        # Affichage du bloc des entetes
        affiche_dataframe_cells(new_df_x,recup_debut_entete(new_df_x),recup_debut_data(new_df_x))


        liste_insertions_colonnes_bilan = recup_colonnes_bilan(new_df_x)
        print("Liste des entetes : ", liste_insertions_colonnes_bilan)
        
        # Insertion des donnees de pays, 
        insert_pays_to_oracle(cursor, liste_insertions_noms_codes_pays)
        insert_banque_to_oracle(cursor, liste_insertions_noms_codes_banques)
        insert_postes_to_oracle(cursor, liste_insertions_postes_bilan)
        insert_colonnes_to_oracle(cursor, liste_insertions_colonnes_bilan)
        
    except oracledb.DatabaseError as error:
        print("Echec de l'insertion ! ", error)
        return [0,df_x.iloc[0,0]]
    except Exception as error:
        print("Insertion non faite. ", error)
        return [0,df_x.iloc[0,0]]
    else:
        connexion.commit()
        print("Insertion reussie !")
        return [1,df_x.iloc[0,0]]


    if cursor:
       cursor.close()
    
    return "Ok"