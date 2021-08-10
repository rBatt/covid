# Covid Trends

Repo is a project to test out using Airflow and PySpark on a Windows machine.

The DAGs in _ariflow_dag_copy are copies from the airflow directory; this application does not run as-is.

I used the scheduled job to update a local postgres database (not needed, but was just testing out the workflow) using PySpark. Originally scheduled to run every day.

The notebook in the covid directory is what I would run to visualize current patterns and trends.