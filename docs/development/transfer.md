# Transfer Interface

## Terminology
- TransferItem: A specific file or directory that needs to be staged in or out for a specific Job id
- TransferTask:  A transfer operation (globus, rsync, ...) between a Site and an external endpoint, which may contain one or many TransferItems
- TransferItem to TransferTask is many-to-one

## Design 
- TransferService should only use high-level TransferInterface methods and task_status_dict
- No knowledge of underlying protocol (nothing in file should be Globus or rsync specific)

```
TransferInterface
	poll_tasks(task_id_list) --> List[task_status_dict]

	submit_task(source_netloc, dest_netloc, source_dir, dest_dir, transfers_list) -> task_id *or* logs error and raises

task_status_dict:
	{
		state: active, inactive, done, error
		error_info: dict
	}
```

## Implementation

```
service/transfer.py
platform/transfer_interface.py
platform/globus_transfer.py
```

### Service Procedure
- database should not store protocol://netloc; only location aliases
- add "inactive" to states: this means user needs to renew credentials in Globus but not an error

TransferItems: filter on site_id, state=pending or active, limit 1000
	fetched list: [ (id, job_id, direction, transfer_location, source path, destination path, state, task_id, transfer_info) ] 

Split apart pending and active items into two lists

For *active or inactive items*:
	Build mapping dict [ task_id ] = list[ transfer_items ]
	Poll each transfer task_id to get task_status_dict 
	If there is a state change, bulk-update transfer_items with state and error_info 

For pending items:

Transfer and preproc should both double-check & ensure workdir existence for each Job

Look at current number of active tasks, settings max_concurrent_transfers and settings transfer_batch_size
Decide to create N transferTasks with batch size of M per task
 
GROUP BY:
	dict [ (direction, transfer_location) ] = [ transfer_list ]

--> Take up to N of the largest (direction, transfer_location) tasks with min(M, len(transfer_list)) items per task

Validate Transfer locations:  
	drop dict keys and log WARNING for any unknown aliases or unsupported protocols

Lookup involved Jobs:  filter by id to get the set of Jobs that will be involved in this Task

For each validated TransferTask:  
	grab the appropriate handler based on protocol (e.g. globus:// --> GlobusInterface)
	Build transfers list: list of (src, dst) path tuples, where src and dst and *relative* to the top-level source_path and destination_path
	Perform submit:
		(source_netloc, source_path, dest_netloc, dest_path, transfers_list)
		If OK: bulk-update TransferItems with TaskID & state
		If raises TransientError: drop it and try again later (no status update)
		If raises CriticalError: set the transferitems state to failed (user will need to fix something and reset state manually)
		Uncaught exceptions will take the service down as expected

## Test 
-> test integration with Globus: create a personal endpoint locally
-> test with a dummy app that:
	--> stages in file from theta_dtn
	--> creates output file in preprocess
	--> bypass launcher by setting state to RUN_DONE
	--> stages out result back to theta_dtn
--> scale this up to 1000 jobs; 5 concurrent transfers; is everything OK?