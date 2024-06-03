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
						contact_id = row[0]
						phone_number = row[1]
						email = row[2]
						linked_id = row[3]
						link_prcd = row[4]
						if link_prcd == "primary":
							consolidated_resp["contact"]["primaryContactId"] = contact_id
							rows_with_same_linked_id_query = "SELECT * FROM contact WHERE linked_id = (%s);" % (contact_id)
							rows_with_same_linked_id_resp = conn.execute(text(rows_with_same_linked_id_query))
							rows_with_same_linked_id_resp_records = rows_with_same_linked_id_resp.fetchall()
							for recd in rows_with_same_linked_id_resp_records:
								recd_contact_id = recd[0]
								recd_phone_number = recd[1]
								recd_email = recd[2]
								if recd_phone_number != "null" and recd_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(recd_phone_number) # add to resp.
								if recd_email != "null" and recd_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(recd_email) # add to resp.
								if recd_contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
									consolidated_resp["contact"]["secondaryContactIds"].append(recd_contact_id)
						elif link_prcd == "secondary":
							if contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
								consolidated_resp["contact"]["secondaryContactIds"].append(contact_id)
							if consolidated_resp["contact"]["primaryContactId"] == None:
								consolidated_resp["contact"]["primaryContactId"] = linked_id
								get_primary_recd_query = "SELECT * FROM contact WHERE id = (%s);" % (linked_id)
								get_primary_recd_resp = conn.execute(text(get_primary_recd_query))
								get_primary_recd_resp_row = get_primary_recd_resp.fetchone()
								pr_phone_number = get_primary_recd_resp_row[1]
								pr_email = get_primary_recd_resp_row[2]
								if pr_email != "null" and pr_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(pr_email) # add to resp.
								if pr_phone_number != "null" and pr_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(pr_phone_number) # add to resp.
						if email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
							consolidated_resp["contact"]["emails"].append(email) # add to resp.
						if phone_number != "null" and phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
							consolidated_resp["contact"]["phoneNumbers"].append(phone_number) # add to resp.
				else:
					# the email recv. in request is not present in the database
					inpt_email_not_present_flag = True
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# process the phone number input
		if phone_number_inpt != "null":
			conn = get_db_connection()
			query = "SELECT * FROM contact WHERE phone_number = ('%s');" % (phone_number_inpt)
			try:
				resp = conn.execute(text(query))
				conn.commit()
				resp_rows = resp.fetchall()
				if len(resp_rows) != 0:
					for row in resp_rows:
						contact_id = row[0]
						phone_number = row[1]
						email = row[2]
						linked_id = row[3]
						link_prcd = row[4]
						if link_prcd == "primary":
							consolidated_resp["contact"]["primaryContactId"] = contact_id
							rows_with_same_linked_id_query = "SELECT * FROM contact WHERE linked_id = (%s);" % (contact_id)
							rows_with_same_linked_id_resp = conn.execute(text(rows_with_same_linked_id_query))
							rows_with_same_linked_id_resp_records = rows_with_same_linked_id_resp.fetchall()
							for recd in rows_with_same_linked_id_resp_records:
								recd_contact_id = recd[0]
								recd_phone_number = recd[1]
								recd_email = recd[2]
								if recd_email != "null" and recd_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(recd_email) # add to resp.
								if recd_phone_number != "null" and recd_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(recd_phone_number) # add to resp.
								if recd_contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
									consolidated_resp["contact"]["secondaryContactIds"].append(recd_contact_id)
						elif link_prcd == "secondary":
							if contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
								consolidated_resp["contact"]["secondaryContactIds"].append(contact_id)
							if consolidated_resp["contact"]["primaryContactId"] == None:
								consolidated_resp["contact"]["primaryContactId"] = linked_id
								get_primary_recd_query = "SELECT * FROM contact WHERE id = (%s);" % (linked_id)
								get_primary_recd_resp = conn.execute(text(get_primary_recd_query))
								get_primary_recd_resp_row = get_primary_recd_resp.fetchone()
								pr_phone_number = get_primary_recd_resp_row[1]
								pr_email = get_primary_recd_resp_row[2]
								if pr_email != "null" and pr_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(pr_email) # add to resp.
								if pr_phone_number != "null" and pr_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(pr_phone_number) # add to resp.
						if phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
							consolidated_resp["contact"]["phoneNumbers"].append(phone_number) # add to resp.
						if email != "null" and email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
							consolidated_resp["contact"]["emails"].append(email) # add to resp.
				else:
					# the phone_number recv. in request is not present in the database
					inpt_phone_number_not_present_flag = True
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# both email and phone_number not present in the database
		if inpt_email_not_present_flag == True and inpt_phone_number_not_present_flag == True:

		consolidated_resp_json = json.dumps(consolidated_resp, indent=4)
		return consolidated_resp_json