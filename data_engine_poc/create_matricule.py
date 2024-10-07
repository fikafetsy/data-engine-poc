import random
import psycopg2
from faker import Faker

# Configuration de la base de données
DB_HOST = "localhost"
DB_NAME = "datawarehouse-dev"
DB_USER = "pguser"
DB_PASSWORD = "pgpwd"

# Initialisation de Faker
fake = Faker()

def generate_employee_data(num_employees=1):
    employees = []
    unique_id = 439  # Démarre l'ID unique à 439

    for i in range(num_employees):
        # Génération d'un matricule
        matricule = f"EMP{str(i + 1).zfill(4)}"  
        
        # Génération d'une date de naissance aléatoire
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=65)  # Entre 18 et 65 ans
        
        # Génération d'un numéro de sécurité sociale
        gender = random.choice([1, 2])  # 1 pour homme, 2 pour femme
        ssn_suffix = random.randint(100000000, 999999999)  # Les 9 derniers chiffres
        social_security_number = f"{gender}{ssn_suffix}"
        
        # Utiliser l'ID unique et l'incrémenter pour le prochain employé
        employee_unique_id = unique_id
        unique_id += 1

        # Création d'un dictionnaire pour l'employé
        employee = {
            'unique_id': employee_unique_id,
            'matricule': matricule,
            'date_of_birth': date_of_birth,
            'social_security_number': social_security_number
        }
        
        employees.append(employee)

    return employees

def insert_employees(employees):
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Insertion des employés
        for employee in employees:
            cursor.execute("""
                INSERT INTO employees_data (unique_id, matricule, date_of_birth, social_security_number)
                VALUES (%s, %s, %s, %s)
            """, (employee['unique_id'], employee['matricule'], employee['date_of_birth'], employee['social_security_number']))

        # Commit des changements
        conn.commit()

    except Exception as e:
        print(f"Erreur lors de l'insertion : {type(e).__name__}")
        print(f"Détails de l'exception : {e}")
    finally:
        cursor.close()
        conn.close()

# Exécution de la fonction
if __name__ == "__main__":
    num_employees_to_generate = 1000  # Ajuste le nombre d'employés à générer
    employee_data = generate_employee_data(num_employees_to_generate)
    insert_employees(employee_data)
    print(f"{num_employees_to_generate} employés ont été générés et insérés dans la base de données.")
