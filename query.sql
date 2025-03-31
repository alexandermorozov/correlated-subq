SELECT 
    d.doctor_id,
    d.first_name,
    d.last_name,
    d.specialty,
    d.email AS doctor_email,
    d.phone AS doctor_phone,
    p.practice_id,
    p.practice_name,
    p.address_line1,
    p.address_line2,
    p.city,
    p.state,
    p.zip_code,
    p.phone AS practice_phone,
    p.email AS practice_email,
    dp.role,
    dp.start_date,
    dp.end_date,
    dp.is_primary,
    l.license_id,
    l.license_number,
    l.license_type,
    l.state AS license_state,
    l.issue_date,
    l.expiry_date,
    l.status AS license_status
FROM 
    doctors d
LEFT JOIN LATERAL (
	SELECT * FROM doctor_practices
	WHERE doctor_practices.doctor_id = d.doctor_id
        AND (end_date IS NULL OR end_date >= CURRENT_DATE)
	LIMIT 1
	) dp on 1 = 1
LEFT JOIN 
    practices p ON dp.practice_id = p.practice_id
LEFT JOIN LATERAL (
	SELECT *
	FROM licenses
	WHERE licenses.doctor_id = d.doctor_id
	LIMIT 1
	) l on 1 = 1
WHERE 
    p.practice_id IS NOT NULL
ORDER BY 
    p.practice_name,
    d.last_name,
    d.first_name;
