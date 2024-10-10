from repository.csv_repository import init_accidents
from flask import Flask

from controllers.crashes_controller import crashes_blueprint

app = Flask(__name__)


if __name__ == "__main__":

    app.register_blueprint(crashes_blueprint, url_prefix="/api/crashes")
    app.run(debug=True)