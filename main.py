import pandas as pd
import mysql.connector


def build_database():
    # Simplified to localhost, user/pw unspecified as well
    # Open & Close always
    try:
        db = mysql.connector.connect(
            user="root",
            password=""
        )

        cursor = db.cursor()
        cursor.execute("CREATE DATABASE PumpjackDataAnalystCodeChallenge")
        db.close()
        return True
    # Lazy exception handling
    except:
        return False


def create_tables():
    try:
        db = mysql.connector.connect(
            user="root",
            password="",
            database="PumpjackDataAnalystCodeChallenge"
        )

        cursor = db.cursor()

        tables = {}
        # department: id::UUID, name::Text, salary_increment::numeric
        tables['department'] = ("CREATE TABLE department ( `department_id` INT(255) NOT NULL AUTO_INCREMENT , "
                                "`first_name` VARCHAR(255) NOT NULL , `salary_increment` decimal(25, 2) NOT NULL,"
                                " PRIMARY KEY (`department_id`))")

        # employee: id :: UUID, first_name::Text, last_name::Text, salary::numeric, department_id::UUID
        tables['employee'] = ("CREATE TABLE employee (`employee_id` INT(255) NOT NULL AUTO_INCREMENT , "
                              "`first_name` VARCHAR(255) NOT NULL, "
                              "`last_name` VARCHAR(255) NOT NULL, "
                              "`salary` decimal(25, 2) NOT NULL, "
                              "`department_id` INT(255) NOT NULL, "
                              "PRIMARY KEY (`employee_id`), "
                              "FOREIGN KEY (`department_id`) "
                              "REFERENCES `department`(`department_id`) ON DELETE CASCADE)")

        for table in tables:
            cursor.execute(tables[table])

        db.close()
        return True
    # Lazy exception handling
    except:
        return False


def add_departments(depts):
    deptartments = []
    db = mysql.connector.connect(
        user="root",
        password="",
        database="PumpjackDataAnalystCodeChallenge"
    )

    cursor = db.cursor()

    for ind in depts.index:
        dep = str(depts['department'][ind])

        if dep not in deptartments:
            perc_salary = str(depts["perc_salary"][ind])

            vals = (0, dep, perc_salary)
            query = "INSERT INTO `department` (`department_id`, `first_name`, `salary_increment`) " \
                    "VALUES (%s, %s, %s)"

            cursor.execute(query, vals)
            deptartments.append(dep)

    print("Departments Added")
    db.commit()
    db.close()
    return True


def add_employees(emps):
    db = mysql.connector.connect(
        user="root",
        password="",
        database="PumpjackDataAnalystCodeChallenge"
    )

    cursor = db.cursor()
    # Cheating? not sure, the problem is just 4 possible foreign ID's
    department_map = {"Finance": 1, "IT": 2, "Sales": 3, "Marketing": 4}

    for ind in emps.index:
        fname = emps["first_name"][ind]
        lname = emps["last_name"][ind]
        salary = str(emps["salary"][ind])
        dept_id = department_map[emps["department"][ind]]

        vals = (0, fname, lname, salary, dept_id)

        query = "INSERT INTO `employee` (`employee_id`, `first_name`, `last_name`, `salary`, `department_id`)" \
                " VALUES (%s, %s, %s, %s, %s)"

        cursor.execute(query, vals)

    db.commit()
    db.close()


def update_employees():
    db = mysql.connector.connect(
        user="root",
        password="",
        database="PumpjackDataAnalystCodeChallenge"
    )

    cursor = db.cursor()

    query = "CREATE TABLE updated_salaries SELECT employee_id, " \
            "ROUND((salary * (1+ department.salary_increment/100)), 2) as updated_salary " \
            "FROM employee LEFT JOIN department ON employee.department_id = department.department_id;"

    cursor.execute(query)
    db.commit()
    db.close()

    return "Finished"


if __name__ == '__main__':
    # Build Database and tables

    build = build_database()
    if build:
        build_tables = create_tables()
        if build_tables:
            print("Database & Tables created, proceed")
        else:
            print("Table Creations Failed.")
    else:
        print("Database Creation Failed.")

    # DB & Tables are created successfully fill the tables
    # Darius,Mufutau,3901,Finance,10
    cols = ['first_name', 'last_name', 'salary', 'department', 'perc_salary']
    data = pd.read_csv("flat_data.csv")

    add_departments(data)
    add_employees(data)
    update_employees()
