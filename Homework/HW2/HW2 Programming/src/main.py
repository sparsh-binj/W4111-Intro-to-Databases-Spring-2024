from typing import Any, Dict

# Simple starter project to test installation and environment.
# Based on https://fastapi.tiangolo.com/tutorial/first-steps/
from fastapi import FastAPI, Response, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
# Explicitly included uvicorn to enable starting within main program.
# Starting within main program is a simple way to enable running
# the code within the PyCharm debugger
import uvicorn

from db import DB

# Type definitions
KV = Dict[str, Any]  # Key-value pairs

app = FastAPI()

# NOTE: In a prod environment, never put this information in code!
# There are design patterns for passing confidential information to
# application.
# TODO: You may need to change the password
db = DB(
    host="localhost",
    port=3306,
    user="root",
    password="dbuserdbuser",
    database="s24_hw2",
)


@app.get("/")
async def healthcheck():
    return HTMLResponse(content="<h1>Heartbeat</h1>", status_code=status.HTTP_200_OK)


# TODO: all methods below

# --- STUDENTS ---

@app.get("/students")
async def get_students(req: Request):
    """Gets all students that satisfy the specified query parameters.

<<<<<<< HEAD
    For instance,
        GET http://0.0.0.0:8002/students
    should return all attributes for all students.
=======
	GET http://0.0.0.0:8002/students?email=dff9@columbia.edu&iq=50

	select * from students where email='dff9@columbia.edu' and iq='50'

	GET http://0.0.0.0:8002/students?email=dff9@columbia.edu&iq=50&fields=last_name,iq

	select last_name, iq from students where email='dff9@columbia.edu' and iq='50'

	For instance,
		GET http://0.0.0.0:8002/students
	should return all attributes for all students.


    For instance,
        GET http://0.0.0.0:8002/students?first_name=John&last_name=Doe
    should return all attributes for students whose first name is John and last name is Doe.

    You must implement a special query parameter, `fields`. You can assume
    `fields` is a comma-separated list of attribute names. For instance,
        GET http://0.0.0.0:8002/students?first_name=John&fields=first_name,email
    should return the first name and email for students whose first name is John.
    Not every request will have a `fields` parameter.

    You can assume the query parameters are valid attribute names in the student table
    (except `fields`).

    :param req: The request that optionally contains query parameters
    :returns: A list of dicts representing students. The HTTP status should be set to 200 OK.
    """

    # Use `dict(req.query_params)` to access query parameters
    query_params = dict(req.query_params)
    rows = query_params.get('fields', [])
    filters: KV = dict(query_params)
    if rows:
        rows = rows.split(',')
        del filters['fields']
    result = db.select(table="student", columns=rows, filters=filters)
    return JSONResponse(content=result, status_code=200)


@app.get("/students/{student_id}")
async def get_student(student_id: int):
    """Gets a student by ID.

    For instance,
        GET http://0.0.0.0:8002/students/1
    should return the student with student ID 1. The returned value should
    be a dict-like object, not a list.

    If the student ID doesn't exist, the HTTP status should be set to 404 Not Found.

    :param student_id: The ID to be matched
    :returns: If the student ID exists, a dict representing the student with HTTP status set to 200 OK.
                If the student ID doesn't exist, the HTTP status should be set to 404 Not Found.
    """
    result = db.select("student", rows=[], filters={'student_id': student_id})
    if result:
        return JSONResponse(content=result[0], status_code=200)
    else:
        return JSONResponse(content="Student record not found!", status_code=404)


@app.post("/students")
async def post_student(req: Request):
    """Creates a student.

    You can assume the body of the POST request is a JSON object that represents a student.
    You can assume the request body contains only attributes found in the student table and does not
    attempt to set `student_id`.

    For instance,
        POST http://0.0.0.0:8002/students
        {
            "first_name": "John",
            "last_name": "Doe",
            ...
        }
    should create a student with the attributes specified in the request body.

    If the email is not specified in the request body, the HTTP status should be set to 400 Bad Request.
    If the email already exists in the student table, the HTTP status should be set to 400 Bad Request.
    If the enrollment year is not valid, the HTTP status should be set to 400 Bad Request.

    :param req: The request, which contains a student JSON in its body
    :returns: If the request is valid, the HTTP status should be set to 201 Created.
                If the request is not valid, the HTTP status should be set to 400 Bad Request.
    """

    # Use `await req.json()` to access the request body
    json_data = await req.json()
    email = json_data.get('email')
    year = json_data.get('enrollment_year')
    email_check = not email or db.select("student", [], filters={"email": email})
    year_check = not year or not (2016 <= int(year) <= 2023)
    if email_check or year_check:
        return JSONResponse(
            content="Invalid input! Please ensure email is included and unique, enrollment year is b/w 2016-2023",
            status_code=400)
    db.insert("student", json_data)
    return JSONResponse("Successfully inserted record!", status_code=201)


@app.put("/students/{student_id}")
async def put_student(student_id: int, req: Request):
    """Updates a student.

    You can assume the body of the PUT request is a JSON object that represents a student.
    You can assume the request body contains only attributes found in the student table and does not
    attempt to update `student_id`.

    For instance,
        PUT http://0.0.0.0:8002/students/1
        {
            "first_name": "Joe"
        }
    should update the student with student ID 1's first name to Joe.

    If the student does not exist, the HTTP status should be set to 404 Not Found.
    If the email is set to null in the request body, the HTTP status should be set to 400 Bad Request.
    If the email already exists in the student table, the HTTP status should be set to 400 Bad Request.
    If the enrollment year is not valid, the HTTP status should be set to 400 Bad Request.

    :param student_id: The ID of the student to be updated
    :param req: The request, which contains a student JSON in its body
    :returns: If the request is valid, the HTTP status should be set to 200 OK.
                If the request is not valid, the HTTP status should be set to the appropriate error code.
    """

    # Use `await req.json()` to access the request body
    json_data = await req.json()
    email = json_data.get('email', "empty")
    year = json_data.get('enrollment_year')
    email_check = email != "empty" and (not email or db.select("student", [], filters={"email": email}))
    year_check = not year or not (2016 <= int(year) <= 2023)
    if not db.select("student", [], filters={"student_id": student_id}):
        return JSONResponse(
            content="Student Record not found!", status_code=404)
    if email_check or year_check:
        return JSONResponse(
            content="Invalid input! Please ensure email is included and unique, enrollment year is b/w 2016-2023",
            status_code=400)
    db.update("student", json_data, {"student_id": student_id})
    return JSONResponse("Successfully updated record!", status_code=200)


@app.delete("/students/{student_id}")
async def delete_student(student_id: int):
    """Deletes a student.

    For instance,
        DELETE http://0.0.0.0:8002/students/1
    should delete the student with student ID 1.

    If the student does not exist, the HTTP status should be set to 404 Not Found.

    :param student_id: The ID of the student to be deleted
    :returns: If the request is valid, the HTTP status should be set to 200 OK.
                If the request is not valid, the HTTP status should be set to 404 Not Found.
    """
    if not db.select("student", [], filters={"student_id": student_id}):
        return JSONResponse(
            content="Student Record not found!", status_code=404)
    db.delete("student", {"student_id": student_id})
    return JSONResponse("Successfully deleted record!", status_code=200)


# --- EMPLOYEES ---

@app.get("/employees")
async def get_employees(req: Request):
    """Gets all employees that satisfy the specified query parameters.

    For instance,
        GET http://0.0.0.0:8002/employees
    should return all attributes for all employees.

    For instance,
        GET http://0.0.0.0:8002/employees?first_name=Don&last_name=Ferguson
    should return all attributes for employees whose first name is Don and last name is Ferguson.

    You must implement a special query parameter, `fields`. You can assume
    `fields` is a comma-separated list of attribute names. For instance,
        GET http://0.0.0.0:8002/employees?first_name=Don&fields=first_name,email
    should return the first name and email for employees whose first name is Don.
    Not every request will have a `fields` parameter.

    You can assume the query parameters are valid attribute names in the employee table
    (except `fields`).

    :param req: The request that optionally contains query parameters
    :returns: A list of dicts representing employees. The HTTP status should be set to 200 OK.
    """

    # Use `dict(req.query_params)` to access query parameters
    query_params = dict(req.query_params)
    rows = query_params.get('fields', [])
    filters: KV = dict(query_params)
    if rows:
        rows = rows.split(',')
        del filters['fields']
    result = db.select(table="employee", columns=rows, filters=filters)
    return JSONResponse(content=result, status_code=200)


@app.get("/employees/{employee_id}")
async def get_employee(employee_id: int):
    """Gets an employee by ID.

    For instance,
        GET http://0.0.0.0:8002/employees/1
    should return the employee with employee ID 1. The returned value should
    be a dict-like object, not a list.

    If the employee ID doesn't exist, the HTTP status should be set to 404 Not Found.

    :param employee_id: The ID to be matched
    :returns: If the employee ID exists, a dict representing the employee with HTTP status set to 200 OK.
                If the employee ID doesn't exist, the HTTP status should be set to 404 Not Found.
    """
    result = db.select("employee", rows=[], filters={'employee_id': employee_id})
    if result:
        return JSONResponse(content=result[0], status_code=200)
    else:
        return JSONResponse(content="Employee record not found!", status_code=404)


@app.post("/employees")
async def post_employee(req: Request):
    """Creates an employee.

    You can assume the body of the POST request is a JSON object that represents an employee.
    You can assume the request body contains only attributes found in the employee table and does not
    attempt to set `employee_id`.

    For instance,
        POST http://0.0.0.0:8002/employees
        {
            "first_name": "Don",
            "last_name": "Ferguson",
            ...
        }
    should create an employee with the attributes specified in the request body.

    If the email is not specified in the request body, the HTTP status should be set to 400 Bad Request.
    If the email already exists in the employee table, the HTTP status should be set to 400 Bad Request.
    If the employee type is not valid, the HTTP status should be set to 400 Bad Request.

    :param req: The request, which contains an employee JSON in its body
    :returns: If the request is valid, the HTTP status should be set to 201 Created.
                If the request is not valid, the HTTP status should be set to 400 Bad Request.
    """

    # Use `await req.json()` to access the request body
    json_data = await req.json()
    email = json_data.get('email')
    job = json_data.get('employee_type')
    email_check = not email or db.select("employee", [], filters={"email": email})
    job_check = not job or job not in ["Professor", "Lecturer", "Staff"]
    if email_check or job_check:
        return JSONResponse(
            content="Invalid input! Please ensure email is included and unique, employee_type is one of Professor / "
                    "Lecturer / Staff",
            status_code=400)
    db.insert("employee", json_data)
    return JSONResponse("Successfully inserted record!", status_code=201)


@app.put("/employees/{employee_id}")
async def put_employee(employee_id: int, req: Request):
    """Updates an employee.

    You can assume the body of the PUT request is a JSON object that represents an employee.
    You can assume the request body contains only attributes found in the employee table and does not
    attempt to update `employee_id`.

    For instance,
        PUT http://0.0.0.0:8002/employees/1
        {
            "first_name": "Donald"
        }
    should update the employee with employee ID 1's first name to Donald.

    If the employee does not exist, the HTTP status should be set to 404 Not Found.
    If the email is set to null in the request body, the HTTP status should be set to 400 Bad Request.
    If the email already exists in the employee table, the HTTP status should be set to 400 Bad Request.
    If the employee type is not valid, the HTTP status should be set to 400 Bad Request.

    :param employee_id: The ID of the employee to be updated
    :param req: The request, which contains an employee JSON in its body
    :returns: If the request is valid, the HTTP status should be set to 200 OK.
                If the request is not valid, the HTTP status should be set to the appropriate error code.
    """

    # Use `await req.json()` to access the request body
    json_data = await req.json()
    email = json_data.get('email', "empty")
    job = json_data.get('employee_type')
    email_check = email != "empty" and (not email or db.select("employee", [], filters={"email": email}))
    job_check = not job or job not in ["Professor", "Lecturer", "Staff"]
    if not db.select("employee", [], filters={"employee_id": employee_id}):
        return JSONResponse(
            content="Employee Record not found!", status_code=404)
    if email_check or job_check:
        return JSONResponse(
            content="Invalid input! Please ensure email is included and unique, employee_type is one of Professor / "
                    "Lecturer / Staff",
            status_code=400)
    db.update("employee", json_data, {"employee_id": employee_id})
    return JSONResponse("Successfully updated record!", status_code=200)


@app.delete("/employees/{employee_id}")
async def delete_employee(employee_id: int):
    """Deletes an employee.

    For instance,
        DELETE http://0.0.0.0:8002/employees/1
    should delete the employee with employee ID 1.

    If the employee does not exist, the HTTP status should be set to 404 Not Found.

    :param employee_id: The ID of the employee to be deleted
    :returns: If the request is valid, the HTTP status should be set to 200 OK.
                If the request is not valid, the HTTP status should be set to 404 Not Found.
    """
    if not db.select("employee", [], filters={"employee_id": employee_id}):
        return JSONResponse(
            content="Employee Record not found!", status_code=404)
    db.delete("employee", {"employee_id": employee_id})
    return JSONResponse("Successfully deleted record!", status_code=200)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
