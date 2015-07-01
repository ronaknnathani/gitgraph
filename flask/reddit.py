import os
CLIENT_ID = os.environ['client_id']
CLIENT_SECRET = os.environ['client_secret']
REDIRECT_URI = "http://52.8.127.252:5000/testgraph"

from flask import Flask
app = Flask(__name__)
@app.route('/')
def homepage():
    text = '<a href="%s">Authenticate with reddit</a>'
    return text % make_authorization_url()

def make_authorization_url():
    # Generate a random string for the state parameter
    # Save it for use later to prevent xsrf attacks
    from uuid import uuid4
    state = str(uuid4())
    save_created_state(state)
    params = {"client_id": CLIENT_ID,
              "response_type": "code",
              "state": state,
              "redirect_uri": REDIRECT_URI,
              "duration": "temporary",
              "scope": "identity"}
    import urllib
    url = "https://ssl.reddit.com/api/v1/authorize?" + urllib.urlencode(params)
    return url

# Left as an exercise to the reader.
# You may want to store valid states in a database or memcache,
# or perhaps cryptographically sign them and verify upon retrieval.
def save_created_state(state):
    pass
def is_valid_state(state):
    return True


if __name__ == '__main__':
    app.run(debug=True, port=65010)
