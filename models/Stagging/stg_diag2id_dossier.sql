{{ config(
    materialized='view',
    schema='ER_DIAG2ID',
    alias='DOSSIER'
) }}

SELECT
    ID_DOSSIER                                 AS ID_DOSSIER,
    ID_DOSSIER_PARENT                          AS ID_DOSSIER_PARENT,
    ID_PCE                                     AS ID_PCE,
    ID_CLIENT                                  AS ID_CLIENT,
    LEFT(CODE_TYPE_DOSSIER, 10)                AS CODE_TYPE_DOSSIER,
    ID_TECHNICIEN                              AS ID_TECHNICIEN,
    ID_CREATEUR                                AS ID_CREATEUR,
    LEFT(CODE_REGION, 50)                      AS CODE_REGION,
    TRY_TO_DATE(DATE_MODIFICATION, 'DD/MM/YYYY HH24:MI:SS') AS DATE_MODIFICATION,
    TRY_TO_DATE(DATE_ENVOI_DIAG, 'DD/MM/YYYY HH24:MI:SS') AS DATE_ENVOI_DIAG,
    TRY_TO_DATE(DATE_RECEPTION_CR, 'DD/MM/YYYY HH24:MI:SS') AS DATE_RECEPTION_CR,
    TRY_TO_DATE(DATE_CREATION, 'DD/MM/YYYY HH24:MI:SS') AS DATE_CREATION,
    LPAD(CODE_STATUT_DOSSIER::TEXT, 3, '0')    AS CODE_STATUT_DOSSIER,
    LEFT(CODE_CATEGORIE_DOSSIER, 16)           AS CODE_CATEGORIE_DOSSIER,
    NOMBRE_RELANCE_TEL                         AS NB_RELANCE_TEL,
    TRY_TO_DATE(DATE_CLOTURE, 'DD/MM/YYYY HH24:MI:SS') AS DATE_CLOTURE

FROM {{ source('diag2id', 'DOSSIER_TEMP') }}
