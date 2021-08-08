# Re-sync
Assignment compatibility check / re-sync tool
Compatibility check between REDIS and the DB, design summary of a  tool that adjusts the compatibility of data when needed
  - Background
  - Approach Strategy
  - Purpose
  - Detailed function design	
    - Compatibility check
    - Compatibility synchronization
  - Implementation


Background
- The counter based assignment method using REDIS operates on the business logic of the WAS and the DB and REDIS are interlocked
  - Redis <-> WAS <-> DB
  - The compatibility between the assignment pointer/count information stored in Redis and the data meta information in the DB must  be matched
- In the process of interlocking, the compatibility between Redis and the DB can break due to disorder / bugs
- A function that re-synchronizes the compatibility is needed additionally to the checking function


Approach Strategy
- According to the assignment logic, the process ored goes from Redis to the DB so any possible loss of data would happen in the DB
  - Check the compatibility by confirming the DB data based on the Redis information
- Data is re-synchronized by comparing Redis and the DB and adding data to wherever the data is missing 
  - If the DB has a data loss, create data and insert into the DB
  - If the Redis count/pointer is missing, add a value
- When the service is live, there needs to be an operation policy to prepare for a situation where the service process updates and the compatibility synchronization process go into competition
  - Compatibility check (read) is always allowed; when there is no load in the DB
  - Compatibility synchronization (write) is allowed when the project is done / not in process


Purpose
- Periodically check and manage the compatibility in terms of system maintenance / operation
- Due to the escalation of the service to a newly launched next-gen platform, the compatibility re-synchronization service was required for the assignment information transfer process



Detailed Function Design

Compatibility Check
- INPUT : Target project ID sorted using the selected target ID
- OUTPUT : 
  - Status statistics provided
    - Assignment state information
  - Compatibility error information
    - Redis / DB difference
  - Error analysis
    - Compatibility error existence
    - Synchronization process info

Compatibility Synchronization
- If the check process result showed a compatibility error
  - Sync? -> Y/N/D
  - Y -> Synchronization proceed / N -> Process done / D -> Dry run of the whole process
- INPUT :  Process continue confirmation
- OUTPUT : 
  - Performance History
    - Redis :
      - key : before -> after
    - DB :
      - Data insert query list
  - After performance, results of a recheck for the compatibility is shown
- Dry run available


Implementation
- Project ID insert value -> Assignment method (single / multiple) confirm -> Task ID gained
- Single assignment :
  - If the Redis counter, DB data count is different
    - Redis counter > DB data count :
      - Find the missing source ID and insert into the DB
      - Set the columns so it can be re-assigned
    - Redis counter < DB data count :
      - Redis key-value info update
- Multiple assignment :
  - Number of assignment for each source
    - Redis counter > DB data count :
      - Find the missing source ID and insert into the DB
    - Redis counter < DB data count :
      - Redis value updated
  - The maximum source ID assigned to each worker
    - Redis assigned maximum source value > DB max :
      - Fill in the missing source ID
    - Redis maximum value < DB max : 
      - Redis value updated

Test Cases : [sync_test_case.pdf](https://github.com/habinlee/Re-sync/files/6950359/sync_test_case.pdf)

Related Documents :
- REDIS basics : [REDIS.pdf](https://github.com/habinlee/Re-sync/files/6950386/REDIS.pdf)
- SSH Tunneling : [SSH Tunneling.pdf](https://github.com/habinlee/Re-sync/files/6950387/SSH.Tunneling.pdf)


