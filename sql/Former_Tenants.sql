SELECT  Tenants.TenantID,                          -- ✅ fixed: was AA.TenantID
        sLocationCode AS LocationCode,
        sSiteName AS SiteName,
        sUnitName AS UnitName,
        sMrMrs AS Salutation,
        sFName AS FirstName,
        sMI AS MI,
        sLName AS LastName,
        sLName + ', ' + sFName AS Name,
        sCompany AS Company,
        sAddr1 AS Address1,
        sAddr2 AS Address2,
        sCity AS City,
        sRegion AS State,
        sPostalCode AS Zipcode,
        sEmail AS Email,
        sPhone AS Phone,
        sAccessCode AS Gatecode,
        dDOB AS Birthdate,
        sTaxID AS TaxID,
        Ledgers.dMovedIn AS DateMovedIn,
        Ledgers.dMovedOut AS DateMovedOut,
        R.ActiveLedgers,
        MIN(sMobile) AS Mobile
FROM Ledgers WITH (NOLOCK)
    INNER JOIN Units   WITH (NOLOCK) ON Units.UnitID   = Ledgers.UnitID
    INNER JOIN Access  WITH (NOLOCK) ON Access.LedgerID = Ledgers.LedgerID
    INNER JOIN Tenants WITH (NOLOCK) ON Tenants.TenantID = Access.TenantID
    INNER JOIN Sites AS S            ON S.SiteID = @MYSITE
    LEFT OUTER JOIN (
        SELECT  AA.TenantID,
                COUNT(LL.LedgerID) AS TotalRentals,
                SUM(CASE WHEN LL.dMovedOut IS NULL THEN 1 ELSE 0 END) AS ActiveLedgers
        FROM Access AA
            INNER JOIN Ledgers LL ON LL.LedgerID = AA.LedgerID
        WHERE AA.SiteID    = @MYSITE
          AND AA.bPrimary  = 1
          AND LL.dDeleted IS NULL
        GROUP BY AA.TenantID
    ) R ON R.TenantID = Tenants.TenantID
WHERE Ledgers.dDeleted   IS NULL
  AND Access.bPrimary     = 1
  AND Ledgers.dMovedOut  IS NOT NULL
  AND Units.bPermanent    = 0
  AND Ledgers.SiteID      = @MYSITE
GROUP BY
    Tenants.TenantID,                              -- ✅ added: was missing
    sLocationCode, sSiteName, sUnitName,
    sMrMrs, sLName, sFName, sMI,
    sCompany, sAddr1, sAddr2, sCity,
    sRegion, sPostalCode, sEmail, sPhone,
    sAccessCode, dDOB, sTaxID,
    Ledgers.dMovedIn, Ledgers.dMovedOut,
    R.ActiveLedgers 
