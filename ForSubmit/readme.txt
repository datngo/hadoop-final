1. Move to folder that includes 2 files data
2. Merge 2 files (inpatient and outpatient) to 1 file
	cat * >> finalData
3. Put finalData to hadoop
   hadoop fs put finalData

4. Execute commands for exercises as below:
Part 1:
   hadoop jar final_part_1.jar finalproject.problem1.ProcessLogs finalData final_part_1_job_1 final_part_1_job_2
   hadoop fs -cat final_part_1_job_2/part-*
 
Part 2:
   hadoop jar final_part_2.jar finalproject.problem2.ProcessLogs finalData final_part_2_job_1 final_part_2_job_2 final_part_2_job_3
   hadoop fs -cat final_part_2_job_3/part-*

Part 3:
   hadoop jar final_part_3.jar finalproject.problem3.ProcessLogs finalData final_part_3_job_1 final_part_3_job_2 final_part_3_job_3 final_part_3_job_4
   hadoop fs -cat final_part_3_job_4/part-*

Part 4:
   hadoop jar final_part_4.jar finalproject.problem4.ProcessLogs finalData final_part_4_job_1 final_part_4_job_2 final_part_4_job_3
   hadoop fs -cat final_part_4_job_3/part-*

