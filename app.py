from flask import Flask, request
from db import get_db_connection, close_db_connection
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import json

app = Flask(__name__)

@app.route("/identity", methods=["POST"])
def identity_handler():
	if request.method == "POST":
		# get the data posted in the HTTP POST request
		post_data = request.get_json()
		email_inpt = str(post_data["email"])
		phone_number_inpt = str(post_data["phoneNumber"])

		# prepare the consolidated response
		consolidated_resp = {
			"contact": {
				"primaryContactId": None,
				"emails": [],
				"phoneNumbers": [],
				"secondaryContactIds": []
			}
		}

		inpt_email_not_present_flag = False
		inpt_phone_number_not_present_flag = False

		# process email input
		if email_inpt != "null":
			conn = get_db_connection()
			query = "SELECT * FROM contact WHERE email = ('%s');" % (email_inpt)
			try:
				resp = conn.execute(text(query))
				conn.commit()

				resp_rows = resp.fetchall()
				if len(resp_rows) != 0:
					for row in resp_rows:
						email = row[2]
						link_prcd = row[4]
						if link_prcd == "primary":
							consolidated_resp["contact"]["primaryContactId"] = row[0]
						if email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
							consolidated_resp["contact"]["emails"].append(email) # add to resp.
				else:
					# the email recv. in request is not present in the database
					inpt_email_not_present_flag = True
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

	consolidated_resp_json = json.dumps(consolidated_resp, indent=4)
	return consolidated_resp_json