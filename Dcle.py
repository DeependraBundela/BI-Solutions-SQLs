#!/usr/bin/env python
import snowflake.connector
import os

# Setting your account information
ACCOUNT = 'account'
USER = 'username'
PASSWORD = 'password'

# Connecting to the Snowflake DB
cnx = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

# Using Database, Schema and Warehouse
cnx.cursor().execute("USE warehouse ETL01_WH")
cnx.cursor().execute("USE NOID.DBO")

# Set Parameters
file_date = '2016-12-26'
client_code = 'FREE'
loan_type = 'CONV'

# Append Decile Calculation Criteria
query = """
        INSERT INTO DBO.DECILE_CRITERIA
        (
          DecileGroupId,
          Model,
          LoanType,
          Cra,
          ClientCode,
          FileDate,
          WhereClause,
          Status,
          CreateDate
        )
        SELECT
            g.DecileGroupId,
            m.Model,
            g.LoanType,
            m.Cra,
            g.ClientCode AS ClientCode,
            m.FileDate AS FileDate,
            g.WhereClause,
            'Not Started' AS Status,
            CURRENT_TIMESTAMP AS CreateDate
        FROM
            DBO.DECILE_GROUP g
        JOIN
            (
                SELECT
                    FileDate,
                          ClientCode,
                                Model,
                                Cra,
                                LoanType
                FROM
                                DBO.SCORE s
                WHERE
                    s.FILEDATE = '{file_date}'
                    --Don't include FAHL Models on non-FAHL Clients
                                AND (Model NOT LIKE 'FAHL%' OR ClientCode = 'FAHL')
                                --Don't include non-FAHL Models for FAHL Client
                    AND (Model LIKE 'FAHL%' OR ClientCode != 'FAHL')
                GROUP BY
                    FileDate,
                          ClientCode,
                                Model,
                                Cra,
                                LoanType
            )m ON m.ClientCode = g.ClientCode  AND m.LoanType = g.LoanType
         WHERE
            --Same Criteria does not already exists
            NOT EXISTS( SELECT 1 FROM DBO.DECILE_CRITERIA c 
                                    WHERE c.DecileGroupId = g.DecileGroupId 
                                          AND c.Model = m.Model 
                                          AND c.LoanType = g.LoanType 
                                          AND c.Cra = m.Cra 
                                          AND c.ClientCode = g.ClientCode 
                                          AND c.FileDate = m.FileDate
                                          AND c.WhereClause = g.WhereClause)
         ORDER BY
             g.WhereClause;
        """.format(**locals())
cnx.cursor().execute(query)

# Truncate Stage
query = """TRUNCATE TABLE STAGE.DECILE""".format(**locals())
cnx.cursor().execute(query)

# Get Pending Decile Calculations
from snowflake.connector import DictCursor
cur = cnx.cursor(DictCursor)
try:
    query = """
            SELECT
                DECILECRITERIAID,
                MODEL,
                LOANTYPE,
                CRA,
                CLIENTCODE,
                FILEDATE,
                WHERECLAUSE
            FROM
                DBO.DECILE_CRITERIA
            WHERE
                Status = 'Not Started'
                AND FileDate = UPPER('{file_date}')
                AND ClientCode = UPPER('{client_code}')
                AND LoanType = UPPER('{loan_type}')
            """.format(**locals())
    decile_criterias = cur.execute(query).fetchall();
finally:
    cur.close()

# Loop through decile criteria
for decile_criteria in decile_criterias:
    #Set Variables
    decile_criteria_id      = decile_criteria['DECILECRITERIAID']
    model                   = decile_criteria['MODEL']
    loan_type               = decile_criteria['LOANTYPE']
    cra                     = decile_criteria['CRA']
    client_code             = decile_criteria['CLIENTCODE']
    file_date               = decile_criteria['FILEDATE']
    where_clause            = decile_criteria['WHERECLAUSE']

    # Prepare Source Query
    query = """
            INSERT INTO STAGE.DECILE
            SELECT
              ScoreId,
              {decile_criteria_id} AS DecileCriteriaId,
              NTILE(10) OVER (ORDER BY Score DESC) AS Decile
            FROM
              (
                SELECT
                    s.ScoreId,
                    s.Score,
                    m.*
                FROM
                    DBO.SCORE s
                JOIN
                    DBO.MODEL m ON m.ModelId = s.ModelId
                WHERE 
                    UPPER(s.LoanType) = UPPER('{loan_type}')
                    AND UPPER(s.Cra) = UPPER('{cra}')
                    AND UPPER(s.ClientCode) = UPPER('{client_code}')
                    AND s.FileDate = '{file_date}'
                    AND UPPER(s.Model) = UPPER('{model}')
                )t
            WHERE
                {where_clause}
                AND FILEID NOT IN (1161)
    """.format(**locals())
    cnx.cursor().execute(query)

    #Set Decile Criteria as Complete
    cnx.cursor().execute("UPDATE DBO.DECILE_CRITERIA SET Status = 'Staged' WHERE DECILECRITERIAID = %s", (decile_criteria_id))

# MERGE STAGE.DECILE INTO DBO.DECILE
query = """
        MERGE INTO DBO.DECILE t USING STAGE.DECILE s ON s.ScoreId = t.ScoreId
        WHEN MATCHED THEN UPDATE SET t.DecileCriteriaId = s.DecileCriteriaId, t.Decile = s.Decile
        WHEN NOT MATCHED THEN INSERT (ScoreId, DecileCriteriaId, Decile) VALUES (s.ScoreId, s.DecileCriteriaId, s.Decile);
        """.format(**locals())
cnx.cursor().execute(query)

#Set Decile Criteria as Complete
cnx.cursor().execute("UPDATE DBO.DECILE_CRITERIA SET Status = 'Complete', CompleteDate = CURRENT_TIMESTAMP WHERE Status = 'Staged'")
