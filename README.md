# Bitespeed_Backend_Task_Identity_Reconciliation
## Candidate Name: Rohan Shah

## Example Usage (Curl Request):
curl -X POST https://bitespeed-backend-task-identity-539j.onrender.com/identity -H "Content-Type: application/json" --data "{\"email\": \"rakesh@hillvalley.edu\", \"phoneNumber\": \"234567\"}"

## Example Response (Curl Response):
{
    "contact": {
        "primaryContactId": 5,
        "emails": [
            "deepika@hillvalley.edu",
            "rakesh@hillvalley.edu",
            "rohan@hillvalley.edu"
        ],
        "phoneNumbers": [
            "34899",
            "345678",
            "234567"
        ],
        "secondaryContactIds": [
            4,
            6,
            7,
            3
        ]
    }
}

## Current Database values (as a csv file):
[Current DB Values](https://github.com/RoSh-23/Bitespeed_Backend_Task_Identity_Reconciliation/blob/master/current_values_in_db.csv)

## Tech Stack used:
### Flask, Python, PostgreSQL
### Hosted on: Render

## Endpoint: https://bitespeed-backend-task-identity-539j.onrender.com/identity
