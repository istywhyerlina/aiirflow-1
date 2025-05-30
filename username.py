from airflow import settings
session = settings.Session()
users = session.execute("SELECT * FROM ab_user").fetchall()
print(users)
session.close()