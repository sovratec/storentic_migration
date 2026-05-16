SELECT Tenants.*, Units.*,  Ledgers.*, Access.* FROM Ledgers With (NOLOCK)
        INNER JOIN Units With (NOLOCK) ON Units.UnitID = Ledgers.UnitID
        INNER JOIN Access With (NOLOCK) ON Access.LedgerID = Ledgers.LedgerID
        INNER JOIN Tenants With (NOLOCK) ON Tenants.TenantID = Access.TenantID
        WHERE  (Ledgers.SiteID = @MYSITE)   --SiteLink will fill in your SiteID(s)	
