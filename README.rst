**************
CeresDB Python
**************

CeresDB Python is a Python SDK for the CeresDB database. To use it, follow the below 
example:

.. code-block:: python

    from ceresdb_python import Connection
    
    # Change these according to your CeresDB instance
    CERESDB_USERNAME = "ceresdb"
    CERESDB_PASSWORD = "ceresdb"
    CERESDB_HOST = "localhost"
    CERESDB_PORT = 7437
    
    conn = Connection(CERESDB_USERNAME, CERESDB_PASSWORD, CERESDB_HOST, CERESDB_PORT)

    data = conn.query("<your AQL query here>")

Data will always be returned as a list of dictionaries with any errors during the query 
being raised. For more information on AQL queries, head `Here<https://ceresdb.readthedocs.io/en/latest/querying.html>`_
