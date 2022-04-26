USE DMS;

DROP TABLE IF EXISTS ##viol_arr_citywide

/* 
- no "violent felony" flag in the NYPD feed 
- solution: match the charges based on the statute and category to planning_charges2 and then use the IsVFO field. 
- use for olbs.dbo.olbs_record because it includes all citywide arrests (dms.dbo.arrest does not )

olbs_record is "wide" table w/ fields for up to 5 arrest charges.
first statement unpivots olbs_record charges into a long table */
;WITH tmp AS (
SELECT 
	source_file, 
	deft_NYSID_num AS NYSID, 
	arr_id_num AS ArrestID, 
	arr_off_taxnum AS AOTax,
	offense_year = YEAR(offense_date),
	offense_date,
	offense_date + ' ' + offense_time AS OffenseDateTime,
	Arr_year = YEAR(Arr_date),
	Arr_date,
	Arr_date + arr_time AS ArrestDateTime,
	arr_boro AS ArrestBorough,
	OutInNY = 0,
	OpenCases = 0,
	OutOnBail = 0,
	Category,
	Charge, 
	ISNULL(IsAttempt,0) AS IsAttempt,
	CASE WHEN RIGHT(Charge,1) = 'X' THEN 1 ELSE 0 END AS IsSMO,
	CASE WHEN RIGHT(Charge,1) = 'H' THEN 1 ELSE 0 END AS IsHateCrime,
	CASE WHEN RIGHT(Charge,1) = 'T' THEN 1 ELSE 0 END AS IsTerror,
	REPLACE(REPLACE(ChargeOrder1, 'chg_lawcode_', ''), 't', '1') AS ChargeOrder
FROM 
/* all felony offenses in 2018 
NOTE: the arr_date in olbs_record is VARCHAR and throws error when filtering as a date.
Solution is to join source_file field in the xml feed to the last_update_source_file in olbs_record */
	(SELECT 
		x.* 
	FROM olbs.dbo.olbs_record_xml x
	JOIN olbs.dbo.olbs_record o ON o.last_update_source_file = x.source_file
	WHERE
/* if we care about incidents of violent felony offenses then the date of the offense and not of the arrest is what matters */
		CONVERT(DATE, x.offense_date) >= '2018-01-01'
	AND o.chg_class_t = 'F'
/* not voided */
	AND ( CASE WHEN x.rec_seal_code IN ('1', '2', '3', '4', '5', '6','V') THEN 0 ELSE 1 END ) = 1
	) arr 
/* go from wide table to long on charge and category fields 
NOTE: charge category fields are prepended as chg_class_ */
	UNPIVOT (Charge FOR ChargeOrder1 IN (chg_lawcode_t, chg_lawcode_2, chg_lawcode_3, chg_lawcode_4, chg_lawcode_5) ) AS code
	UNPIVOT (Category FOR ChargeOrder2 IN (chg_class_t, chg_class_2, chg_class_3, chg_class_4, chg_class_5) ) AS category
	UNPIVOT (IsAttempt FOR ChargeOrder3 IN (chg_attind_t, chg_attind_2, chg_attind_3, chg_attind_4, chg_attind_5) ) AS attempt
	WHERE
		RIGHT(ChargeOrder1,1)= RIGHT(ChargeOrder2,1) -- match the correct category to the correct charge using the new "charge order" field (t, 2, 3, 4, 5)
	AND RIGHT(ChargeOrder1,1) = RIGHT(Chargeorder3,1)
	)
/* 
charge info is coded diff in olbs_record from planning_charges2 
- recode to make match-able
*/ 
, chg AS (
SELECT DISTINCT
	Charge, 
	CASE WHEN ( LEN(Charge) = 9 AND RIGHT(Charge, 1) LIKE '[1-9]' ) OR ( LEN(Charge) = 10 AND RIGHT(Charge, 2) LIKE '0[1-9]' )  
			THEN 'PL ' + LEFT(REPLACE(Charge, 'PL ', ''), 3) + '.' + SUBSTRING(REPLACE(Charge, 'PL ', ''), 4, 2) + '(' + RIGHT(Charge, 1) + ')'
		WHEN  LEN(Charge) = 10 AND RIGHT(Charge, 2) LIKE '[1][0-9]' 
			THEN 'PL ' + LEFT(REPLACE(Charge, 'PL ', ''), 3) + '.' + SUBSTRING(REPLACE(Charge, 'PL ', ''), 4, 2) + '(' + RIGHT(Charge, 2) + ')'
			WHEN  LEN(Charge) = 10 AND RIGHT(Charge, 2) LIKE '[1-9][A-z]' 
			THEN 'PL ' + LEFT(REPLACE(Charge, 'PL ', ''), 3) + '.' + SUBSTRING(REPLACE(Charge, 'PL ', ''), 4, 2) + '(' + SUBSTRING(REPLACE(Charge, 'PL ', ''), 6, 1) + ')'
	ELSE 'PL ' + LEFT(REPLACE(Charge, 'PL ', ''), 3) + '.' + SUBSTRING(REPLACE(Charge, 'PL ', ''), 4, 2) END AS ChargeNew,
	IsAttempt,
	IsSMO,
	IsTerror,
	IsHateCrime
FROM tmp
WHERE
	Charge LIKE 'PL%'
AND Category = 'F'
	)
/* 
- recode to make planning_charges2 chargeClean matchable
- filter IsOfficeViolent = 1 OR IsVFO = 1
*/
, mtch AS (
SELECT DISTINCT
	chg.Charge, 
	chg.ChargeNew,
	ChargeClean,
	IsOfficeViolent,
	IsVFO,
	chg.IsAttempt,
	chg.IsSMO,
	chg.IsTerror,
	chg.IsHateCrime
FROM chg
JOIN (SELECT 
		ChargeClean, 
		/* recode charges for matching */
		CASE WHEN SUBSTRING(REPLACE(ChargeClean, '110/', ''), 11, 1) LIKE '[1-9]' 
			  AND SUBSTRING(REPLACE(ChargeClean, '110/', ''), 12, 1) LIKE '[0-9]'  
			  AND SUBSTRING(REPLACE(ChargeClean, '110/', ''), 13, 1) = ')'
				THEN LEFT(REPLACE(ChargeClean, '110/', ''), 13)
			WHEN SUBSTRING(REPLACE(ChargeClean, '110/', ''), 11, 1) LIKE '[1-9]' 
			 AND SUBSTRING(REPLACE(ChargeClean, '110/', ''), 12, 1) = ')' 
				THEN LEFT(REPLACE(ChargeClean, '110/', ''), 12)
			ELSE LEFT(REPLACE(ChargeClean, '110/', ''), 9) END
		  AS ChargeNew, 
/*edge cases...PL 120.05 charges are weird in olbs and don't match...non-attempts are vio and attempts are non-vio so going to manually specify here */
		CASE WHEN ChargeClean LIKE 'PL 120.05%' THEN 1 ELSE IsOfficeViolent END AS IsOfficeViolent,
		CASE WHEN ChargeClean LIKE 'PL 120.05%' THEN 1 ELSE IsVFO END AS IsVFO,
		IsAttempt,
		IsSMO,
		IsTerrorism,
		IsHateCrime
	FROM Planning_Charges2 ch
	JOIN ChargeModification cm ON cm.ChargeModificationId = ch.ChargeModificationId
	WHERE
		Category = 'Felony'
	AND ( isOfficeViolent = 1 OR IsVFO = 1 )
	AND (CASE WHEN ch.IsActive = 0 AND LastUpdateTime <= '2018-01-08' THEN 0 
			  WHEN ch.ChargeClean LIKE 'PL 110/120.05%' THEN 0 ELSE 1 END ) = 1
	) pch ON chg.ChargeNew = pch.ChargeNew 
AND pch.IsAttempt = chg.IsAttempt
AND pch.IsSMO = chg.IsSMO
AND pch.IsTerrorism = chg.IsTerror
AND pch.IsHateCrime = chg.IsHateCrime
) 
/* grab all arrests pulled in the tmp and relevant violent charges (VFO or OfficeViolent) */
SELECT DISTINCT
	tmp.*,
	IsOfficeViolent,
	IsVFO,
	ChargeDescription = CAST(NULL AS VARCHAR(200)),
	ChargeCategory    = CAST(NULL AS VARCHAR(200)),
	TopCatPendingDany = CAST(NULL AS VARCHAR(200)),
	TopChgPendingDany = CAST(NULL AS VARCHAR(200)),
	TopTxtPendingDany = CAST(NULL AS VARCHAR(500))
INTO ##viol_arr_citywide
FROM tmp
JOIN mtch on mtch.Charge = tmp.Charge
WHERE
	mtch.IsAttempt = tmp.IsAttempt
AND mtch.IsSMO = tmp.IsSMO
AND mtch.isTerror = tmp.IsTerror
AND mtch.IsSMO = tmp.IsSMO


/* remove multi-arrest records for related offenses 
..group on NYSID, offense date, arrest date, arrest boro AND arresting officer tax */
;WITH tmp AS (
SELECT
NYSID, 
OffenseDateTime,
ArrestDateTime, 
ArrestBorough AS Boro, 
AOTax,
MIN(ArrestID) As frstArrest
FROM ##viol_arr_citywide
WHERE NYSID IS NOT NULL
GROUP BY NYSID, OffenseDatetime, ArrestDateTime, ArrestBorough, AOTax
HAVING COUNT(DISTINCT ArrestId) > 1
) 
DELETE FROM ##viol_arr_citywide WHERE ArrestID IN (
SELECT
vio.ArrestID
FROM ##viol_arr_citywide vio
JOIN tmp ON tmp.NYSID = vio.NYSID 
WHERE
	 tmp.ArrestDateTime= vio.ArrestDateTime 
AND tmp.OffenseDateTime = vio.OffenseDateTime
AND tmp.Boro = vio.ArrestBorough AND tmp.AoTax = vio.AOTax
AND vio.ArrestID > tmp.frstArrest
)


/*inserting charge description and charge category*/
UPDATE ##viol_arr_citywide
SET ChargeDescription = pc.chargeDescription,
    ChargeCategory = pc.category
FROM ##viol_arr_citywide vio
JOIN planning_charges2 pc on REPLACE(chargeCode, 'PL', 'PL ') = vio.Charge




/* pull dany cases + details where the defendant was at liberty following cc arraignment 
- add info for defendants that committed a crime listed in the above viol_crimes_citywide table during the pendancy 
of their case */
IF OBJECT_ID('tempdb.dbo.##dany_atliberty', 'U') IS NOT NULL
DROP TABLE ##dany_atliberty

;WITH Tmp AS (
SELECT DISTINCT
smry.NYSID,
smry.DefendantId,
CCArraignDate AS ArcDate,
smry.InstantCmid,
smry.InstantCaseType,
InstTopCat,
arcReleaseStatus = arc.ReleaseStatus, 
ArcBailSet = bailSetAmt,
dsp.Disposition,
dsp.DispoDate,
SUBSTRING((SELECT DISTINCT ' / ' + vio.Charge AS [text()] 
			FROM ##viol_arr_citywide vio 
			WHERE
				vio.NYSID = smry.NYSID
			AND CONVERT(DATE, vio.OffenseDateTime) > arc.ArcDate
			AND CONVERT(DATE, vio.OffenseDateTime) <= COALESCE(dsp.DispoDate, CONVERT(DATE, GetDate()))
			FOR XML PATH(''), ELEMENTS), 4, 2000) AS ViolArrCharges
FROM dms.dbo.Planning_DefSummary2 smry 
JOIN dms.dbo.Planning_Arraignments2 arc ON arc.PlanningArraignmentsID = smry.CCArraignID
LEFT JOIN (SELECT * 
		   FROM dms.dbo.Planning_Dispositions2 
		   WHERE EventOrder = 1
		   AND ISNULL(InterimDispoType, 'null') <> 'Partial Conviction'
		   ) dsp On dsp.DefendantId = smry.DefendantId
WHERE
	arc.ArcDate >= '2018-01-01'
AND CASE WHEN ArcSurvive = 0 THEN 0 ELSE 1 END = 1
AND arc.ReleaseStatus IN ('Parole', 'Bail', 'Bail With Curfew',  'ROR', 'ROR With Curfew', 
						'Supervised Release', 'Intensive Community Monitoring')
)
, cnt AS (
SELECT
DefendantId, 
COUNT(DISTINCT ArrestId) AS VioArrests
FROM tmp
JOIN ##viol_arr_citywide vio ON vio.NYSID = tmp.NYSID
WHERE
	CONVERT(DATE, vio.OffenseDateTime) > tmp.ArcDate
AND CONVERT(DATE, vio.OffenseDateTime) <= COALESCE(tmp.DispoDate, CONVERT(DATE, GetDate()))
GROUP BY tmp.DefendantId
),
 cntMan AS (
SELECT
DefendantId, 
COUNT(DISTINCT ArrestId) AS VioArrests_Man
FROM tmp
JOIN ##viol_arr_citywide vio ON vio.NYSID = tmp.NYSID
WHERE
	CONVERT(DATE, vio.OffenseDateTime) > tmp.ArcDate
AND CONVERT(DATE, vio.OffenseDateTime) <= COALESCE(tmp.DispoDate, CONVERT(DATE, GetDate()))
AND vio.ArrestBorough = 'M'
GROUP BY tmp.DefendantId
)
SELECT 
NYSID,
tmp.DefendantId,
YEAR(ArcDate) AS ArcYear,
ArcDate,
DispoDate,
InstantCaseType,
InstantCMID,
InstTopCat = CASE WHEN instTopCat = 'Felony' AND isVFO = 1 THEN 'Violent Felony'
				  WHEN instTopCat = 'Felony' AND isVFO = 0 THEN 'Non-Violent Felony'
				  ELSE instTopCat END,
ch.ChargeClean AS InstTopChg,
ch.ChargeDescription AS InstTopTxt,
ch.MajorGroup AS InstTopGrp,
ch.isVFO AS InstantViolent,
ch.IsOfficeViolent AS InstantOfficeViolent,
ArcReleaseStatus,
ArcBailSet,
ISNULL(VioArrests,0) AS VioArrests,
ISNULL(VioArrests_Man,0) AS VioArrests_Man,
ViolArrCharges,
HasBurg2 = CASE WHEN ViolArrCharges LIKE '%PL 14025%' THEN 1 ELSE 0 END,
ArcBailCat = CASE WHEN ArcBailSet BETWEEN 2 AND 500 THEN '2-500'
				  WHEN ArcBailSet BETWEEN 501 AND 1000 THEN '501-1000'
				  WHEN ArcBailSet BETWEEN 1001 AND 2500 THEN '1001-2500'
				  WHEN ArcBailSet BETWEEN 2501 AND 5000 THEN '2501-5000'
				  WHEN ArcBailSet BETWEEN 5001 AND 7500 THEN '5001-7500'
				  WHEN ArcBailSet BETWEEN 7501 AND 10000 THEN '7501-10000'
				  WHEN ArcBailSet >10000 THEN 'Over 10000'
				ELSE 'No Bail Set' END,
[crimHis(Conviction)] = 0,
[crimHis(FelConv)] = 0,
[crimHis(MisdConv)] = 0,
[crimHis(ViolationConv)] = 0
INTO ##dany_atliberty
FROM Tmp
JOIN Planning_Charges2 ch ON ch.ChargeModificationId = Tmp.InstantCmid
LEFT JOIN cnt On cnt.DefendantId = Tmp.DefendantId
LEFT JOIN cntMan On cntMan.defendantId = Tmp.defendantId



/* update fields in city violent arrests table to indicate whether the arrest was committed
by someone at liberty in nyc */
UPDATE ##viol_arr_citywide
SET OutInNy = 1,
	OutOnBail = CASE WHEN ArcBailSet > 1 THEN 1
					 WHEN arcReleaseStatus = 'Bail' THEN 1 
					ELSE 0 END
FROM ##viol_arr_citywide vio
JOIN ##dany_atliberty dany on dany.NYSID = vio.NYSID
WHERE
	CONVERT(DATE, vio.OffenseDateTime) > dany.ArcDate 
AND CONVERT(DATE, vio.OffenseDateTime) <= COALESCE(dany.DispoDate, CONVERT(DATE, Getdate()))



/* number of open cases at DANY at the time of arrest */
;WITH tmp AS (
SELECT
	vio.ArrestID,  
	COUNT(DISTINCT DefendantId) AS Cases
FROM ##viol_arr_citywide vio
JOIN (SELECT 
		vio.ArrestID, 
		dany.NYSID, 
		dany.DefendantId
	  FROM ##viol_arr_citywide vio
	  JOIN ##dany_atliberty dany on dany.NYSID = vio.NYSID
	  WHERE
		  CONVERT(DATE, vio.OffenseDateTime) > dany.ArcDate 
	  AND CONVERT(DATE, vio.OffenseDateTime) <= COALESCE(dany.DispoDate, CONVERT(DATE, Getdate()))
	) opn ON opn.ArrestID = vio.ArrestID
GROUP BY vio.ArrestID
)
UPDATE ##viol_arr_citywide
SET OpenCases = Cases
FROM ##viol_arr_citywide vio
JOIN tmp ON tmp.ArrestID = vio.ArrestID

;WITH mostsev AS (
SELECT
vio.*,
dl.InstTopCat,
dl.InstTopChg,
dl.InstTopTxt,
LEAD(dl.InstantCMID, 1) OVER(PARTITION BY vio.ArrestID ORDER BY ch.CatClassOrder DESC, ch.Score) AS TopChg
FROM ##viol_arr_citywide vio 
JOIN ##dany_atliberty dl ON dl.NYSID = vio.NYSID
JOIN Dms.dbo.Planning_charges2 ch On ch.ChargeModificationID = dl.InstantCMID
WHERE OutInNY = 1
AND vio.offense_date BETWEEN dl.arcDate AND COALESCE(dl.DispoDate, CONVERT(DATE, Getdate()))
AND ChargeOrder = 1
)
UPDATE ##viol_arr_citywide
SET TopCatPendingDany = ms.InstTopCat,
	TopChgPendingDany = ms.InstTopChg,
	TopTxtPendingDany = ms.InstTopTxt
FROM ##viol_arr_citywide vio
JOIN mostsev ms on ms.ArrestID = vio.ArrestID


/*criminal history - using planning_convictions2 while rap data is unavailable  */
;WITH cvt AS (
		SELECT DISTINCT
			   dany.NYSID,
		       COUNT(DISTINCT cvt.defendantID) as Convictions
		FROM planning_convictions2 cvt
		JOIN ##dany_atliberty dany On dany.nysid = cvt.nysid
		WHERE cvt.DefendantId <> dany.defendantID
		GROUP BY dany.NYSID
)

UPDATE ##dany_atliberty
SET [crimHis(Conviction)] = cvt.convictions
FROM ##dany_atliberty dany
JOIN cvt on cvt.nysid = dany.nysid


;WITH Felcvt AS (
		SELECT DISTINCT
			   dany.NYSID,
		       COUNT(DISTINCT cvt.defendantID) as Convictions
		FROM planning_convictions2 cvt
		JOIN ##dany_atliberty dany On dany.nysid = cvt.nysid
		WHERE cvt.DefendantId <> dany.defendantID
		AND ConvTopCat = 'Felony'
		GROUP BY dany.NYSID
)
UPDATE ##dany_atliberty
SET [crimHis(FelConv)] = Felcvt.convictions
FROM ##dany_atliberty dany
JOIN Felcvt on Felcvt.nysid = dany.nysid



;WITH Misdcvt AS (
		SELECT DISTINCT
			   dany.NYSID,
		       COUNT(DISTINCT cvt.defendantID) as Convictions
		FROM planning_convictions2 cvt
		JOIN ##dany_atliberty dany On dany.nysid = cvt.nysid
		WHERE cvt.DefendantId <> dany.defendantID
		AND ConvTopCat = 'Misdemeanor'
		GROUP BY dany.NYSID
)
UPDATE ##dany_atliberty
SET [crimHis(MisdConv)] = Misdcvt.convictions
FROM ##dany_atliberty dany
JOIN Misdcvt on Misdcvt.nysid = dany.nysid





;WITH Violcvt AS (
		SELECT DISTINCT
			   dany.NYSID,
		       COUNT(DISTINCT cvt.defendantID) as Convictions
		FROM planning_convictions2 cvt
		JOIN ##dany_atliberty dany On dany.nysid = cvt.nysid
		WHERE cvt.DefendantId <> dany.defendantID
		AND ConvTopCat = 'Violation/Infraction'
		GROUP BY dany.NYSID
)
UPDATE ##dany_atliberty
SET [crimHis(ViolationConv)] = Violcvt.convictions
FROM ##dany_atliberty dany
JOIN Violcvt on Violcvt.nysid = dany.nysid


/* REMINDER: This table includes all arrests occuring in NYC (5 boros) since 2018 and all the office violent or VFO charges associated with each arrest 
so may have more than one entry per ArrestID */
SELECT
*
FROM ##viol_arr_citywide
WHERE OutInNY = 1 AND TopCatPendingDany IS NULL



/* REMINDER: This table includes all cases arraigned at DANY since 2018 and indicates the number of 
violent arrests that occurred while the case was pending (and the related charges) (one row per defendantId) */
SELECT
*
FROM ##dany_atliberty
ORDER BY VioArrests DESC


---distribution of the number of arrests per person while they were at liberty following arc
;WITH dis AS (
SELECT DISTINCT
       VioArrests_Man AS [Number of Arrests While at Liberty],
	   COUNT(DISTINCT dany.nysid) AS [Number of Persons]
FROM ##dany_atliberty dany
GROUP BY VioArrests_Man
--ORDER BY VioArrests DESC
)
SELECT DISTINCT *,
       Pct = (SELECT [Number of Persons] FROM dis d2
					WHERE d1.[Number of Arrests While at Liberty]= d2.[Number of Arrests While at Liberty])* 100.0 /
					(SELECT SUM([Number of Persons]) FROM dis d2)
FROM dis d1
ORDER BY [Number of Arrests While at Liberty] ASC



---arrest alert, cvt history for those who had 4 or more arrests while at liberty
DROP TABLE ##dany_vio_detail

SELECT DISTINCT 
       dany.NYSID,
	   levelone AS ArrestAlert,
	   [crimHis(Conviction)],
	   [crimHis(FelConv)],
	   [crimHis(MisdConv)],
	   [crimHis(ViolationConv)],
	   GunCasesSince2018 = 0
INTO ##dany_vio_detail
FROM ##dany_atliberty dany
JOIN arrestalert.dbo.ArrestAlertLevelLinkNYSID arr on arr.NYSID = dany.nysid
JOIN arrestalert.dbo.ArrestAlertLevelOneLU one ON one.LevelOneID = arr.LevelOneID
WHERE VioArrests_Man >= 4
 

 --- gun cases
;WITH temp AS (
SELECT DISTINCT
     pdef.nysid,
	 GunCases = COUNT(DISTINCT defendantID)
FROM ##dany_vio_detail dany
JOIN planning_defSummary2 pdef on pdef.nysid = dany.nysid
JOIN planning_charges2 pc on pc.chargemodificationid = pdef.instantCmid
WHERE pc.chargeClean LIKE '%265.%'
  AND YEAR(pdef.arrestDate) >= 2018
GROUP BY pdef.nysid
)
UPDATE ##dany_vio_detail
SET GunCasesSince2018 = GunCases
FROM ##dany_vio_detail detail 
JOIN temp on temp.nysid = detail.nysid



SELECT * FROM ##dany_vio_detail
--- cvt history for those who had 4 or more arrests while at liberty

