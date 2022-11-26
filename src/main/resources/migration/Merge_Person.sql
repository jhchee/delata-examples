MERGE INTO delta.`/tmp/delta/user`
USING userupdates
ON `user``.id = userupdates.id
WHEN MATCHED THEN
  UPDATE SET
    id = userupdates.id,
    first_name = userupdates.first_name,
    last_name = userupdates.last_name,
    salary = userupdates.salary
    updated_on = userupdates.updated_on
WHEN NOT MATCHED
  THEN INSERT (
    id,
    firstName,
    middleName,
    lastName,
    gender,
    birthDate,
    ssn,
    salary
  )
  VALUES (
    userupdates.id,
    userupdates.firstName,
    userupdates.middleName,
    userupdates.lastName,
    userupdates.gender,
    userupdates.birthDate,
    userupdates.ssn,
    userupdates.salary
  )