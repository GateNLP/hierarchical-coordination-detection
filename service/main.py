import os

from coordination.webapp import app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("WEBAPP_PORT", "5000")), debug=True)
