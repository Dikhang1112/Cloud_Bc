from ETL_System import create_app

import os

app = create_app()

if __name__ == "__main__":
    app_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
    app.run(debug=True)