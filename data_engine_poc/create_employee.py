import psycopg2
from faker import Faker

# Configuration de la base de données
DB_HOST = "localhost"
DB_NAME = "datawarehouse-dev"
DB_USER = "pguser"
DB_PASSWORD = "pgpwd"

# Initialisation de Faker
fake = Faker()

# Fonction pour générer des employés
def generate_employees(num_employees=1000):
    employees = []
    emails = set()  # Pour suivre les e-mails uniques
    while len(employees) < num_employees:
        employee = {
            'name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address(),
            'job': fake.job(),
            'salary': fake.random_int(min=30000, max=120000)
        }
        # Si l'e-mail n'est pas déjà pris, l'ajouter à la liste
        if employee['email'] not in emails:
            employees.append(employee)
            emails.add(employee['email'])  # Ajouter l'e-mail à l'ensemble

    return employees


# Fonction pour insérer des employés dans la base de données
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
                INSERT INTO employees (name, email, phone, address, job, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (employee['name'], employee['email'], employee['phone'], employee['address'], employee['job'], employee['salary']))

        # Commit des changements
        conn.commit()

    except Exception as e:
        print(f"Erreur lors de l'insertion : {e}")
    finally:
        cursor.close()
        conn.close()

# Exécution du script
if __name__ == "__main__":
    employees = generate_employees(1000)
    insert_employees(employees)
    print("1000 employés ont été générés et insérés dans la base de données.")
