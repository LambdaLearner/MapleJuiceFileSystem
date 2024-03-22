import socket
import threading
import json
import hashlib
import os
import subprocess
import pickle
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from math import ceil

from log import setup_logger
import ast
from fd4 import *
from fs4 import *

import re

class MichaelJackson():
    '''
    This is our MapleJuice Class for MP4. This class handles all the operations related to MJ.
    '''
    def __init__(self, sdfs_obj, fd_obj, fm_obj):
        self.dict_lock = threading.RLock() #lock for mj_dict
        self.mj_queue = deque()
        self.mj_dict = {}

        self.sql_dict_lock = threading.RLock() #lock for mj_dict
        self.sql_queue = deque()
        self.sql_dict = {}
        
        self.maple_com_msg_dict = {}
        self.maple_com_msg_lock = threading.RLock()

        self.maple_lock = threading.RLock() #lock for maple_dict, task_dict
        # self.task_dict = {} #common for maple and juice
        self.maple_task_dict = {}
        self.maple_key_dict = {}
        self.maple_keys_set = set() #maple_lock used
        
        self.juice_lock = threading.RLock()
        self.juice_task_dict = {}
        self.juice_key_dict = {}
        self.juice_keys_set = set() #juice_lock used        

        self.fd_obj = fd_obj
        self.fm_obj = fm_obj
        self.leader = "fa23-cs425-8201.cs.illinois.edu"
        self.host_name = socket.gethostname()

        self.sdfs_obj = sdfs_obj
    
    def get_target_col_idx(self, col_name, use_2= False):
    #     column_list = ['index', 'X', 'Y', 'OBJECTID', 'Intersecti', 'UPS', 'Coord_Type',
    #    'CNTRL_Seri', 'CNTRL_Mode', 'Number_of_', 'Detection_', 'Interconne',
    #    'Percent_St', 'Year_Timed', 'LED_Status', 'CNTRL_Vers', 'Cabinet_Ty',
    #    'CNTRL_Note', 'Install_Da', 'Black_Hard', 'Year_Paint', 'Countdown_',
    #    'All_Red_Fl', 'Condition', 'ConditionDate', 'InstallDate',
    #    'WarrantyDate', 'LegacyID', 'FACILITYID', 'Ownership',
    #    'OwnershipPercent', 'LED_Installed_Year', 'Controller_ID', 'Notes',
    #    'RepairYear', 'FieldVerifiedDate']

    #     col_list_2 = ["index","City","State"]

        column_list = ['ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat',
       'Start_Lng', 'End_Lat', 'End_Lng', 'Distance(mi)', 'Description',
       'Street', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone',
       'Airport_Code', 'Weather_Timestamp', 'Temperature(F)', 'Wind_Chill(F)',
       'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Direction',
       'Wind_Speed(mph)', 'Precipitation(in)', 'Weather_Condition', 'Amenity',
       'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
       'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
       'Turning_Loop', 'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
       'Astronomical_Twilight']
    
        col_list_2 = ['Weather', 'Weather_bin', "Channel"]

        if use_2:
            return col_list_2.index(col_name)


        return column_list.index(col_name)

    def sql_queue_manager(self):
        '''
        RUN ONLY ON LEADER!

        This Queue Manager, manages SQL TASKS requested by the user.

        '''

        while(True):
            time.sleep(10)
            with self.sql_dict_lock:    
                #when done pop elements from queue
                if len(self.sql_queue) == 0:
                    continue
                k = self.sql_queue.popleft()
                operation = self.sql_dict[k].split(" ")[2] #first element is juice_req or maple_req - beware!
                
            if operation == "sql":
                print("running sql executor")
                with self.sql_dict_lock:
                    sql_data = self.sql_dict[k]
                self.run_sql_operation(sql_data) #send all active vms list of operations to complete!
                # self.reset_maple_dict()
                # self.multicast_final_maple_map()
                with self.sql_dict_lock:
                    self.sql_dict.pop(k)
            
    def run_sql_operation(self, sql_cmd):
        '''
        Helper function to sql_queue to parse SQL queries and automate MJ tasks.
        '''

        parenthesis_stack = []
        dataset_stack = []
        operation_stack = []
        #sample sql cmd : "sql_req sql select all from dataset where ( conditions ) -- NORMAL/REGEX"
        
        filter_type = sql_cmd.split("--")[1].strip()
        sql_cmd = sql_cmd.split("--")[0].strip()
        if filter_type == "NORMAL":
            
            dataset = sql_cmd.split("FROM")[1].split("WHERE")[0].strip()
            where_conditions = sql_cmd.split("WHERE")[1].strip()
            where_conditions_list = where_conditions.split(" ")
            count = 0
            for element in where_conditions_list:
                if element == "(":
                    parenthesis_stack.append(element)
                elif element == ")" and len(dataset_stack) >= 2 and len(operation_stack) >=1:
                    print("running mjoa")
                    operator = operation_stack.pop()
                    data1 = dataset_stack.pop()
                    data2 = dataset_stack.pop()
                    new_dataset= self.run_mj_on_operator(data1, data2, operator, count)
                    dataset_stack.append(new_dataset)
                    count = count + 1
                    parenthesis_stack.pop()

                elif element in ["AND", "OR"]:
                    operation_stack.append(element)
                elif len ( element.split("=") ) == 2:
                    new_dataset = self.run_mj_on_condition(element, dataset, count)
                    print("running mj")
                    count = count + 1
                    dataset_stack.append(new_dataset)
            
            
            #TODO : run put operation on last dataset
            # sql_final_file = dataset_stack.pop()
            # self.sdfs_obj.put_file_operation(local_file_name = sql_final_file, sdfsfilename= sql_final_file, mj_op = True)
        
        elif filter_type == "REGEX":
            st = time.time()
            print(f"----------------------{time.time()}--------------------------")
            dataset = sql_cmd.split("FROM")[1].split("WHERE")[0].strip()
            regex_conditions = sql_cmd.split("WHERE")[1].strip()
            regex_conditions_list = regex_conditions.split(" ")
            count = 0

            for element in regex_conditions_list:
                if element == "(":
                    parenthesis_stack.append(element)
                    
                elif element == ")" and len(dataset_stack) >= 2 and len(operation_stack) >=1:
                    print("running mjoa")
                    operator = operation_stack.pop()
                    data1 = dataset_stack.pop()
                    data2 = dataset_stack.pop()
                    new_dataset= self.run_mj_on_operator(data1, data2, operator, count)
                    dataset_stack.append(new_dataset)
                    count = count + 1
                    parenthesis_stack.pop()

                elif element == ")":
                    print("Regex execution complete!")
                elif element in ["AND", "OR"]:
                    operation_stack.append(element)
                else:
                    print(f"Regex string : {element}")
                    element = element.replace("#s", " ")
                    new_dataset = self.run_mj_on_regex(element, dataset, count)
                    print("running mj")
                    count = count + 1
                    dataset_stack.append(new_dataset)
            
            print(f"----------------------{time.time()}--------------------------")
            en = time.time()
            #TODO : run put operation on last dataset
            # sql_final_file = dataset_stack.pop()
            # self.sdfs_obj.put_file_operation(local_file_name = sql_final_file, sdfsfilename= sql_final_file, mj_op = True)

            print(f"REGEX NOT HANDLED LOL : {en - st}")
        
        elif filter_type == "JOIN":
            #sql select all FROM s1 JOIN s2 WHERE ( s1.Ownership=s2.City ) -- JOIN
            st = time.time()
            d1,d2 = sql_cmd.split("FROM")[1].split("WHERE")[0].split("JOIN")
            where_conditions = sql_cmd.split("WHERE")[1].strip()
            where_conditions_list = where_conditions.split(" ")
            count = 0
            for element in where_conditions_list:
                if element == "(":
                    parenthesis_stack.append(element)
                elif element == ")" and len(dataset_stack) >= 2 and len(operation_stack) >=1:
                    print("running mj_join")
                    operator = operation_stack.pop()
                    data1 = dataset_stack.pop()
                    data2 = dataset_stack.pop()
                    new_dataset= self.run_mj_on_operator(data1, data2, operator, count)
                    dataset_stack.append(new_dataset)
                    count = count + 1
                    parenthesis_stack.pop()

                elif element in ["AND", "OR"]:
                    operation_stack.append(element)
                elif len ( element.split("=") ) == 2:
                    new_dataset = self.run_mj_on_join_condition(element, count)
                    print("running mj")
                    count = count + 1
                    dataset_stack.append(new_dataset)
            en = time.time()

            print(f"TIME FOR JOINNNN : {en - st}")
                
    def run_mj_on_join_condition(self, join_condition, count):
        '''
        Helper Function to run SQL Query. Runs this if the user wants to match a column with a particular value.
        '''
        s1,s2 = join_condition.split("=")
        d1, col1 = s1.split(".")
        d2, col2 = s2.split(".")

        extra_info = {}
        extra_info["col1"] = col1
        extra_info["col2"] = col2
        
        cmd_str = f"maple_req no maple mj_join 9 d{count} {d1},{d2}"
        self.run_maple_operation(cmd_str, extra_info)
        self.reset_maple_dict()
        ts = int(time.time())
        mj_op_file = f"sql_join_{ts}.s"
        cmd_str = f"juice_req juice mj_join 9 d{count} {mj_op_file} 1"
        self.run_juice_operation(cmd_str, extra_info, put_file_in_sdfs = False)
        self.reset_juice_dict()

        return mj_op_file




    def run_mj_on_regex(self, regex_str, dataset_name, count):
        '''
        Helper Function to run SQL REGEX Query..
        '''
        extra_info = {}
        extra_info["regex_str"] = regex_str
        extra_info["put_val_in_sdfs"] = False

        cmd_str = f"maple_req no maple t2_mjreg 5 d{count} {dataset_name}"
        self.run_maple_operation(cmd_str, extra_info)
        self.reset_maple_dict()
        ts = int(time.time())
        mj_op_file = f"sql_{ts}.s"
        cmd_str = f"juice_req juice t2_mjreg 5 d{count} {mj_op_file} 1"
        self.run_juice_operation(cmd_str, extra_info, put_file_in_sdfs = False)
        self.reset_juice_dict()
        return mj_op_file


    def run_mj_on_operator(self, d1, d2, operator, count):
        '''
        Helper Function to run nested AND/OR conditions in the Query..
        '''
        extra_info = {}
        extra_info["put_val_in_sdfs"] = False
        extra_info["operator"] = operator

        cmd_str = f"maple_req no maple t2_mjoa 8 d{count} {d1},{d2}"
        self.run_maple_operation(cmd_str, extra_info, get_file_location = "sdfs")
        self.reset_maple_dict()
        ts = int(time.time())
        mj_op_file = f"sql_{ts}.s"
        cmd_str = f"juice_req juice t2_mjoa 8 d{count} {mj_op_file} 1"
        self.run_juice_operation(cmd_str, extra_info, put_file_in_sdfs = False)
        self.reset_juice_dict()
        return mj_op_file


    def run_mj_on_condition(self, filter_string, dataset_name, count):
        '''
        Helper Function to run SQL Query. In case there's a col and we want to match the value to that.
        '''
        target_col = filter_string.split("=")[0].strip()
        target_val = filter_string.split("=")[1].strip()
        extra_info = {}
        extra_info["target_col"] = target_col
        extra_info["target_val"] = target_val
        extra_info["put_val_in_sdfs"] = False

        cmd_str = f"maple_req no maple t2_mj 8 d{count} {dataset_name}"
        self.run_maple_operation(cmd_str, extra_info)
        self.reset_maple_dict()
        ts = int(time.time())
        mj_op_file = f"sql_{ts}.s"
        cmd_str = f"juice_req juice t2_mj 8 d{count} {mj_op_file} 1"
        self.run_juice_operation(cmd_str, extra_info, put_file_in_sdfs = False)
        self.reset_juice_dict()
        return mj_op_file


    def queue_manager(self):
        '''
        RUN ONLY ON LEADER!

        Queue Manager which manages all user entered MJ tasks
        '''

        while(True):
            time.sleep(10)
            with self.dict_lock:    
                #when done pop elements from queue
                if len(self.mj_queue) == 0:
                    continue
                k = self.mj_queue.popleft()
                operation = self.mj_dict[k][0].split(" ")[2] #first element is juice_req or maple_req - beware!
                
            if operation == "maple":
                print("running maple executor")
                with self.dict_lock:
                    mj_data = self.mj_dict[k][0]
                    extra_info = self.mj_dict[k][1]
                self.run_maple_operation(mj_data, extra_info) #send all active vms list of operations to complete!
                self.reset_maple_dict()
                # self.multicast_final_maple_map()
                with self.dict_lock:
                    self.mj_dict.pop(k)
            elif operation == "juice":
                print("running juice executor")
                with self.dict_lock:
                    mj_data = self.mj_dict[k][0]
                    extra_info = self.mj_dict[k][1]
                self.run_juice_operation(mj_data, extra_info) #send all active vms list of operations to complete!
                self.reset_juice_dict()
                # self.multicast_final_maple_map()
                with self.dict_lock:
                    self.mj_dict.pop(k)
        
    def multicast_messages(self, message_data_host_list, d2 = {}):
        '''
        Helper Function to multicast messages to other VMs in the membership list.
        '''
        UDP_PORT = 1234
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            for i in message_data_host_list:
                pickled_data = pickle.dumps((i[0], i[1], d2))
                sock.sendto(pickled_data, (socket.gethostbyname(i[2]), UDP_PORT))

    def reset_maple_dict(self):
        with self.maple_lock:
            self.maple_task_dict = {}
            self.maple_com_msg_dict = {}
        return
    
    def reset_juice_dict(self):
        with self.juice_lock:
            self.juice_task_dict = {}
        with self.maple_lock:
            self.maple_key_dict = {}
            self.maple_keys_set = set()
            
        return
    
    

    def get_lines_in_file(self, filename_list, get_file_location):

        ans = {}
        for f in filename_list:
            fn = f"/home/sb82/mp4_82/{get_file_location}/" + f
            result = subprocess.run(['wc', '-l', fn], stdout=subprocess.PIPE, universal_newlines=True)
            ans[f] = int(result.stdout.split()[0])
        return ans

    def run_juice_operation(self, cmd_string, extra_info={}, put_file_in_sdfs = True):
        """
        Leader only!
        Function which assigns Juice tasks to active VMs and monitors the membership list for failures.
        It reassigns the task to other VMs in case of failures.
        """
        cmd_string = cmd_string.split(" ")

        delete_files = cmd_string[-1]
        file_name = cmd_string[-2]
        intermediate_file_name = cmd_string[-3]
        num_juices = int(cmd_string[-4])
        mj_exe = cmd_string[-5]
        delete_input = cmd_string[-1]
        if delete_input == "0":
            delete_input = False
        else:
            delete_input = True 
        
        num_keys = len(self.maple_keys_set)

        initial_task_list = []
        j = 1
        msg = "Run_Juice"
        with self.fd_obj.lock:
            shard_list = [1]
            mem_list = self.fd_obj.membership_list.keys()
            per_machine_shard = max(1, int(num_juices / (len(mem_list)-1) ))
            if num_juices > (len(list(mem_list)) - 1 ):
                shard_list = [i for i in range(1, per_machine_shard + (num_juices % (len(mem_list) - 1)) + 1 )]
            for i in mem_list:
                if i == self.leader:
                    continue
                task_dict = {"status" : "In Progress", 
                            "task_id" : j,
                            "output_file_name" : file_name,
                            "cmd_string" : cmd_string,
                            "operation" : "juice",
                            "shard_list" : shard_list,
                            "per_machine_shard_size" : ceil(len(self.maple_keys_set) / num_juices),
                            "intermediate_file_name" : intermediate_file_name,
                            "mj_exe" : mj_exe,
                            "keys_list" : list(self.maple_keys_set),
                            "vm_key_dict" : self.maple_key_dict,
                            "extra_info" : extra_info
                        }
                individual_set = (msg, task_dict, i)
                initial_task_list.append(individual_set)
                shard_list = [shard_list[-1] + i for i in range(1,per_machine_shard+1)]
                # self.task_dict[j] = task_dict
                j = j + 1
                self.juice_task_dict[i] = task_dict
                if j > num_juices:
                    break
        self.multicast_messages(initial_task_list)
        
        #TODO add code for failure handling
        while True:
            time.sleep(10)
            tasks_running = False 
            flag = False
            rerun_tasks_list = []
            with self.juice_lock:
                with self.fd_obj.lock:
                    failed_vm_list = list ( set(self.juice_task_dict.keys()) - set(self.fd_obj.membership_list.keys()) )
                    free_vm_list = list( set(self.fd_obj.membership_list.keys()) - set([self.leader]) - set([i for i in self.juice_task_dict.keys() if self.juice_task_dict[i]["status"] == "In Progress"]) )
                    if len ( failed_vm_list ) > 0 and len(free_vm_list) > len(failed_vm_list):
                        for i in failed_vm_list:
                            new_assigned_vm_name = free_vm_list[0]
                            task_set = ("Run_Juice", self.juice_task_dict[i], new_assigned_vm_name)
                            
                            # self.task_dict[self.maple_task_dict[i]["task_id"]] = (new_assigned_vm_name, self.maple_task_dict[i])
                            self.juice_task_dict[new_assigned_vm_name] = self.juice_task_dict[i]
                            self.juice_task_dict.pop(i)

                            rerun_tasks_list.append(task_set)

                        flag = True

                if flag == True:
                    self.multicast_messages(rerun_tasks_list)
                    continue
                    
                for k in list(self.juice_task_dict.keys()):
                    if self.juice_task_dict[k]["status"] == "In Progress":
                        tasks_running = True
                        break
            if tasks_running == False:
                # if put_file_in_sdfs:
                # self.sdfs_obj.put_file_operation(local_file_name = file_name, sdfsfilename= file_name, mj_op = True)
                # multicast_messages
                break
        
        print("ALL JUICE COMPLETED")
        if delete_files == "1":
            #message_data_host_list
            mess = []
            with self.fd_obj.lock:
                mem_list = self.fd_obj.membership_list.keys()
            for k in mem_list:
                mess.append(("Delete_Maple_Files", task_dict, k))
            self.multicast_messages(mess)
            self.sdfs_obj.delete_mj_files(task_dict)
            time.sleep(10)

    


    def run_maple_operation(self, cmd_string, extra_info = {}, get_file_location = "sdfs"):
        """
        Leader only function!
        Function which assigns Maple tasks to active VMs and monitors the membership list for failures.
        It reassigns the task to other VMs in case of failures.
        """

        cmd_string = cmd_string.split(" ")


        file_name = cmd_string[-1]
        filename_list = [file_name]
        multiple_files = False
        if len(file_name.split(',')) > 1:
            filename_list = file_name.split(',')
        
        intermediate_file_name = cmd_string[-2]
        num_maples = int(cmd_string[-3])
        mj_exe = cmd_string[-4]
        param = cmd_string[-6]
        

        num_lines_in_file_dict = self.get_lines_in_file(filename_list, get_file_location)
        initial_task_list = []
        j = 1
        msg = "Run_Maple"
        with self.fd_obj.lock:
            shard_list = [1]
            mem_list = self.fd_obj.membership_list.keys()

            per_machine_shard = max(1, int(num_maples / (len(mem_list)-1) ))
            if num_maples > (len(list(mem_list)) - 1 ):
                shard_list = [i for i in range(1, per_machine_shard + (num_maples % (len(mem_list) - 1)) + 1 )]
            for i in mem_list:
                if i == self.leader:
                    continue
                task_dict = {"status" : "In Progress", 
                            "task_id" : j,
                            "filename_list" : filename_list,
                            "cmd_string" : cmd_string,
                            "operation" : "maple",
                            "shard_list" : shard_list,
                            "intermediate_file_name" : intermediate_file_name,
                            "mj_exe" : mj_exe,
                            "num_lines_in_file_dict" : num_lines_in_file_dict,
                            "lines_per_shard": [ceil(n / num_maples) for n in num_lines_in_file_dict.values()],
                            "extra_info" : extra_info,
                            "get_file_location" : get_file_location,
                            "param" : param,
                            "default_start_point" : 0

                }
                individual_set = (msg, task_dict, i)
                initial_task_list.append(individual_set)
                shard_list = [shard_list[-1] + i for i in range(1,per_machine_shard+1)]
                # self.task_dict[j] = task_dict
                j = j + 1
                self.maple_task_dict[i] = task_dict
                if j > num_maples:
                    break

        self.multicast_messages(initial_task_list)
        
        while True:
            time.sleep(10)
            tasks_running = False 
            flag = False
            rerun_tasks_list = []
            with self.maple_lock:
                with self.fd_obj.lock:
                    failed_vm_list = list ( set(self.maple_task_dict.keys()) - set(self.fd_obj.membership_list.keys()) )
                    free_vm_list =  list ( set(self.fd_obj.membership_list.keys()) - set([self.leader]) - set([i for i in self.maple_task_dict.keys() if self.maple_task_dict[i]["status"] == "In Progress"]) )
                    if len ( failed_vm_list ) > 0 and len(free_vm_list) > len(failed_vm_list):
                        for i in failed_vm_list:
                            new_assigned_vm_name = free_vm_list[0]
                            task_set = ("Run_Maple", self.maple_task_dict[i], new_assigned_vm_name)
                            
                            # self.task_dict[self.maple_task_dict[i]["task_id"]] = (new_assigned_vm_name, self.maple_task_dict[i])
                            self.maple_task_dict[new_assigned_vm_name] = self.maple_task_dict[i]
                            self.maple_task_dict.pop(i)

                            rerun_tasks_list.append(task_set)

                        flag = True

                if flag == True:
                    self.multicast_messages(rerun_tasks_list)
                    continue
                    
                for k in list(self.maple_task_dict.keys()):
                    if self.maple_task_dict[k]["status"] == "In Progress":
                        tasks_running = True
                        break
            if tasks_running == False:
                for i in self.maple_com_msg_dict.keys():
                    already_fetch_keys = []
                    for jk in self.maple_com_msg_dict[i]:
                        self.process_maple_complete_message(jk[0], jk[1], already_fetch_keys)
                        already_fetch_keys = list(set(already_fetch_keys) | set(jk[1]["file_keys"].keys()))



                print("MAPLE IS COMPLETED")
                return


    def append_to_sdfs_file(self, file_name, new_part_dir = "temp_files" ):
        with open(f'/home/sb82/mp4_82/{new_part_dir}/{file_name}', 'r') as source_file, open(f'/home/sb82/mp4_82/sdfs/{file_name}', 'a') as destination_file:
            content = source_file.read()
            destination_file.write(content)
            
    def queue_rcvd_maple_complete_message(self, word_dict, host_dict):
        '''
        Function which queues the received Maple Complete messages, for the leader.
        '''
        vm_id = list(host_dict.keys())[0]

        with self.maple_com_msg_lock:
            if self.maple_com_msg_dict.get(vm_id, []) == []:
                self.maple_com_msg_dict[vm_id] = [[word_dict, host_dict]]
            else:
                self.maple_com_msg_dict[vm_id].append([word_dict, host_dict])
        
        with self.maple_lock:
            if self.maple_task_dict.get(vm_id, {}) != {}:
                self.maple_task_dict[vm_id]["status"] = "Completed"
        
        

    def process_maple_complete_message(self, word_dict, host_dict, already_fetch_keys):
        """
        Only on leader.
        Processes completed Maple messages from VMs and puts the intermediate files in the SDFS 
        """
        vm_id = list(host_dict.keys())[0]
        op = "put"
        for w in list(set(list(host_dict["file_keys"].keys())) - set(already_fetch_keys)):
            op = "put"
            #get file locally!
            self.get_file_locally(w, "temp_files", "maple_files", vm_id)
            if os.path.exists(f"/home/sb82/mp4_82/sdfs/" + w):
                op = "append"
                self.append_to_sdfs_file(w)
                
            if op == "put":
                location = "temp_files"
            else:
                location = "sdfs"
        
            with self.maple_lock:
                self.sdfs_obj.put_file_operation(local_file_name = w, sdfsfilename= w, mj_op = False, directory=location)

        with self.maple_lock:
            if self.maple_task_dict.get(vm_id, {}) != {}:
                self.maple_task_dict[vm_id]["status"] = "Completed"
                for w in word_dict.keys():
                    if len ( self.maple_key_dict.get(w, []) ) != 0:
                        self.maple_key_dict[w].append(vm_id)
                    else:
                        self.maple_key_dict[w] = [vm_id]
                # self.maple_key_dict[vm_id] = word_dict

                self.maple_keys_set = self.maple_keys_set | set(word_dict.keys())

        return

    def process_juice_complete_message(self, output_dict, host_dict):
        """
        Only on leader
        Processes completed Juice messages from VMs and removes (if mentioned) the intermediate files from the SDFS 
        """
        print("received final juice output")
        vm_id = list(host_dict.keys())[0]
        task_dict = host_dict[vm_id]

        op = "put"
        output_file_name = task_dict["output_file_name"]
        
        #get file locally!
        self.get_file_locally(output_file_name, "temp_files", "juice_files", vm_id)
        if os.path.exists(f"/home/sb82/mp4_82/sdfs/" + output_file_name):
            op = "append"
            self.append_to_sdfs_file(output_file_name)
    
        if op == "put":
            location = "temp_files"
        else:
            location = "sdfs"
    
        with self.juice_lock:
            self.sdfs_obj.put_file_operation(local_file_name = output_file_name, sdfsfilename= output_file_name, mj_op = False, directory=location)

        
        with self.juice_lock:
            self.juice_task_dict[vm_id]["status"] = "Completed"
        # fp = "/home/sb82/mp4_82/juice_files/" + task_dict["output_file_name"]
        # with open(fp, 'a') as f_:
        #     for k in output_dict.keys():
        #         if host_dict.get("store_as", "dict") == "dict":
        #             f_.write(f'{[k, output_dict[k]]}\n')
        #         else:
        #             f_.write(k)
        
    def get_maple_files_from_other_vms(self, file_name, is_error_handling = False, key = ""):
        
        ip = socket.gethostbyname(self.leader)
        self_ip = socket.gethostbyname(self.host_name)
        usr = "sb82"
        sanitized_key = ""

        if is_error_handling:
            sanitized_key = key
            if "/" in key:
                sanitized_key = key.replace("/", "_")
            if key.strip() == '':
                sanitized_key = str(key.count(" ")) + "_numspace_str"
            file_name = file_name.split("_")[0] + "_" + sanitized_key + ".maple"
        
        # print(file_name)

        if os.path.exists(f"/home/sb82/mp4_82/sdfs/" + file_name) == False:
            scp = ["scp",'-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'{usr}@{ip}:/home/{usr}/mp4_82/sdfs/{file_name}', f'{usr}@{self_ip}:/home/{usr}/mp4_82/sdfs/{file_name}']
            
            scp_run = subprocess.run(scp, check=True)
        
        return

    def wc_process_maple_file_juice_output(self, task_dict, file_name, key, existing_output_dict = {}):
        #assuming only wc runs
        fp = "/home/sb82/mp4_82/sdfs/" + file_name
        file_ = open(fp, 'r')
        cnt = 0
        for line in file_:
            line = line.strip()
            set_l = ast.literal_eval(line)
            if set_l[0] == key:
                cnt = cnt + 1
        return cnt

    def t1_j1_process_maple_file_juice_output(self, task_dict, file_name, key, existing_output_dict = {}):
        '''
        Juice exe
        '''
        #assuming only wc runs
        sanitized_key = key
        if "/" in key:
            sanitized_key = key.replace("/", "_")
        if key.strip() == '':
            sanitized_key = str(key.count(" ")) + "_numspace_str"
        file_name = task_dict["intermediate_file_name"] + "_" + sanitized_key + ".maple"
        fp = "/home/sb82/mp4_82/sdfs/" + file_name
        file_ = open(fp, 'r')
        cnt = 0
        for line in file_:
            line = line.strip()
            set_l = ast.literal_eval(line)
            if set_l[0] == key:
                cnt = cnt + 1
        return cnt

    def t1_j2_process_maple_file_juice_output(self, task_dict, file_name, key, existing_output_dict = {}, total_count = 0):
        fp = "/home/sb82/mp4_82/sdfs/" + file_name
        file_ = open(fp, 'r')
        print("Running j2")
        for line in file_:
            print(line)

            # line = line.strip()

            # print(line)
            one, det_tuple = line.split('\t')
            detection, freq = ast.literal_eval(det_tuple)
            # set_l = ast.literal_eval(line)
            # print("yyyyy")
            # print(set_l)
            freq = freq.strip()
            existing_output_dict[detection] = int ( freq )
            total_count = total_count + int ( freq )
            
        return existing_output_dict, total_count
    
    def write_juice_output_to_disk_test2(self, output_dict, task_dict):
        output_file_name = task_dict["output_file_name"]
        output_file_path = f'/home/sb82/mp4_82/juice_files/{output_file_name}'
        with open(output_file_path, 'a') as f_:
            for k in output_dict.keys():
                for i in output_dict[k]:
                    f_.write(i + '\n')


    def write_juice_output_to_disk_modified(self, output_dict, task_dict):
        output_file_name = task_dict["output_file_name"]
        output_file_path = f'/home/sb82/mp4_82/juice_files/{output_file_name}'
        with open(output_file_path, 'a') as f_:
            for k in output_dict.keys():
                f_.write(k)

    def write_juice_output_to_disk_join(self, output_dict, task_dict):
        output_file_name = task_dict["output_file_name"]
        output_file_path = f'/home/sb82/mp4_82/juice_files/{output_file_name}'
        with open(output_file_path, 'a') as f_:
            for k in output_dict["fin_output"]:
                f_.write(k)

    def write_juice_output_to_disk(self, output_dict, task_dict, test1 = False):
        output_file_name = task_dict["output_file_name"]
        output_file_path = f'/home/sb82/mp4_82/juice_files/{output_file_name}'
        with open(output_file_path, 'a') as f_:
            for k in output_dict.keys():
                if test1:
                    f_.write('%s\t%s\n' % (k, output_dict[k]))
                else:
                    f_.write(f'({k},{output_dict[k]})')

    def t2_j_process_maple_file_juice_output(self, task_dict, file_name, key):

        file_name = task_dict["intermediate_file_name"] + "_" + "1" + ".maple"
        fp = "/home/sb82/mp4_82/sdfs/" + file_name
        file_ = open(fp, 'r')
        fin_list = []
        for line in file_:
            line = line.strip()
            val = line.split('\t')[1]
            # set_l = ast.literal_eval(line)[0]
            fin_list.append(val)
            # if new_dict.get(set_l[0], []) == []:
            #     new_dict[set_l[0]] = [set_l[1]]
            # else:
            #     new_dict[set_l[0]].append(set_l[1])
        return fin_list

        # IDENTITY REDUCER

        # file_name = task_dict["intermediate_file_name"] + "_" + key + ".maple"
        # fp = "/home/sb82/mp4_82/juice_files/" + file_name
        
        # return 1

    def t2_joa_process_maple_file_juice_output(self, task_dict, file_name, key):
        fp = "/home/sb82/mp4_82/sdfs/" + file_name
        file_ = open(fp, 'r')
        num_lines = 0
        for line in file_:
            line = line.strip()
            num_lines = num_lines + 1
        return num_lines
    
    def jjoin_process_maple_file_juice_output(self, task_dict, file_name, k, existing_output_dict={}):
        line_list = []
        sanitized_key = k
        new_dict = {}   
        if "/" in k:
            sanitized_key = k.replace("/", "_")
        if k.strip() == '':
            sanitized_key = str(k.count(" ")) + "_numspace_str"
        
        sanitized_key = sanitized_key.replace(" ", "space")
        
        file_name = task_dict["intermediate_file_name"] + "_" + sanitized_key + ".maple"
        fp = "/home/sb82/mp4_82/sdfs/" + file_name
        file_ = open(fp, 'r')
        # f_.write(f'{[sanitized_v, [idx, line]]}\n')
        for line in file_:
            line = line.split('\t')

            key, dr = line[0], line[1]
            # print(type(dr))
            # print(dr)
            # print(ast.literal_eval(dr))
            data_marker, row = ast.literal_eval(dr)
            # row = ",".join(row)

            if new_dict.get(data_marker, []) == []:
                new_dict[data_marker] = [row]
            else:
                new_dict[data_marker].append(row)
        
        return new_dict



    def execute_rcvd_juice_cmd(self, task_dict):
        # print("executing juice")
        # print("below is rcvd dict")
        # print(task_dict)
        keys_to_process = set()
        for i in task_dict["shard_list"]:
            keys_to_process = keys_to_process | set(task_dict["keys_list"][(i-1) * task_dict["per_machine_shard_size"] : (i) * task_dict["per_machine_shard_size"]])
        print("keys_processing : ")
        print(keys_to_process)
        output_dict = {}
        temp_dict = {}
        total_count = 0
        store_as = "dict"
        for k in keys_to_process:
            file_name = task_dict["intermediate_file_name"] + "_" + str(k) + ".maple"
            # for h in task_dict["vm_key_dict"][k]:
                
            if task_dict["mj_exe"] == "wc":
                self.get_maple_files_from_other_vms(file_name)
                juice_output = self.wc_process_maple_file_juice_output(task_dict, file_name, k)
                output_dict[k] = output_dict.get(k, 0) + juice_output #specific to wc
            
            elif task_dict["mj_exe"] == "t1_mj1":
                self.get_maple_files_from_other_vms(file_name, is_error_handling = True, key = k)
                juice_output = self.t1_j1_process_maple_file_juice_output(task_dict, file_name, k)
                output_dict[k] = output_dict.get(k, 0) + juice_output
            
            elif task_dict["mj_exe"] == "t1_mj2":
                self.get_maple_files_from_other_vms(file_name)
                output_dict, total_count = self.t1_j2_process_maple_file_juice_output(task_dict, file_name, k, existing_output_dict = output_dict, total_count = total_count)
            
            elif task_dict["mj_exe"] in ["t2_mj", "t2_mjreg"]:
                row_id = k
                # if task_dict["mj_exe"] == "t2_mj":
                #     row_id = k.split(",")[0]
                file_name = task_dict["intermediate_file_name"] + "_" + str(1) + ".maple"
                self.get_maple_files_from_other_vms(file_name)
                output_dict[k] = self.t2_j_process_maple_file_juice_output(task_dict, file_name, k)
                store_as = "line"

            elif task_dict["mj_exe"] == "t2_mjoa":
                row_id = k.split(",")[0]
                file_name = task_dict["intermediate_file_name"] + "_" + str(row_id) + ".maple"
                self.get_maple_files_from_other_vms(file_name)
                juice_output = self.t2_joa_process_maple_file_juice_output(task_dict, file_name, k)
                store_as = "line"
                temp_dict[k] = temp_dict.get(k,0) + juice_output
            
            elif task_dict["mj_exe"] == "mj_join":
                sanitized_key = k
                if "/" in k:
                    sanitized_key = k.replace("/", "_")
                if k.strip() == '':
                    sanitized_key = str(k.count(" ")) + "_numspace_str"
                
                sanitized_key = sanitized_key.replace(" ", "space")
                file_name = task_dict["intermediate_file_name"] + "_" + sanitized_key + ".maple"
                self.get_maple_files_from_other_vms(file_name)
                output_dict[k] = self.jjoin_process_maple_file_juice_output(task_dict, file_name, k)
        
        if task_dict["mj_exe"] == "mj_join":
            
            
            kl = list(output_dict.keys())
            fin_output = []
            for key in kl:
                output_dict[key]["fin_output"] = []
                for k in output_dict[key].get(0, []):
                    for l in output_dict[key].get(1, []):
                        output_dict[key]["fin_output"].append(k.strip() + "," + l)
                fin_output = fin_output + output_dict[key]["fin_output"]
                output_dict.pop(key)
            
            output_dict["fin_output"] = fin_output

        if task_dict["mj_exe"] == "t2_mjoa":
            for k in temp_dict.keys():
                if temp_dict[k] == 2 and task_dict["extra_info"]["operator"] == "AND":
                    output_dict[k] = 1
                elif task_dict["extra_info"]["operator"] == "OR":
                    output_dict[k] = 1

        if task_dict["mj_exe"] == "t1_mj2":
            output_dict = dict(zip(list(output_dict.keys() ), [round(i * 100 / total_count, 2) for i in list ( output_dict.values() ) ] ) )
        
        if task_dict["mj_exe"] in ["mj_join"]:
            self.write_juice_output_to_disk_join(output_dict, task_dict)
            output_dict = {}

        elif task_dict["mj_exe"] in ["t2_mjoa"]:
            self.write_juice_output_to_disk_modified(output_dict, task_dict)
        
        elif task_dict["mj_exe"] in ["t2_mjreg", "t2_mj"]:
            self.write_juice_output_to_disk_test2(output_dict, task_dict)
            output_dict = {}
        else:
            self.write_juice_output_to_disk(output_dict, task_dict, test1 =True)
            output_dict = {}

        self.multicast_messages([("juice_completed", output_dict, self.leader)], d2= {self.host_name : task_dict , "store_as" : store_as})


    def wc_run_maple_executable(self, line, task_dict, word_keys = {}):
        word_list = line.strip().split()
                    
        for word in word_list:
            word = word.lower()
            intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_{word}.maple'
            word_keys[word] = 1
            with open(intermediate_filename, 'a') as word_file:
                word_file.write(f'{[word, 1]}\n')
        return word_keys
    
    def t1_m1_run_maple_executable(self, line, task_dict, word_keys = {}, file_keys = {}):
        
        interc_idx = 10
        det_idx = 9
        x = task_dict["param"]
        if x == "no":
            print("Changing parameter to default = Fiber")
            x = "Fiber"

        row = line.strip().split(',')

        interconne = row[interc_idx]
        detection = row[det_idx]
        
        if interconne == x:
            sanitized_v = detection
            if "/" in detection:
                sanitized_v = detection.replace("/", "_")
            if detection.strip() == '':
                sanitized_v = str(detection.count(" ")) + "_numspace_str"
            intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_{sanitized_v}.maple'
            word_keys[detection] = 1
            file_keys[f'{task_dict["intermediate_file_name"]}_{sanitized_v}.maple'] = 1
            with open(intermediate_filename, 'a') as f_:
                f_.write(f"{[detection, 1]}\n")
        return word_keys, file_keys

    def t1_m2_run_maple_executable(self, line, task_dict, word_keys = {}, file_keys = {}):
        word_keys[1] = 1
        # line = line.strip()
        detection, freq = line.split('\t')
        dt =  (detection, freq)
        # print(line)
        # print(type(line))
        # print(ast.literal_eval(line))
        # line_list = line.split(',')
        
        intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_1.maple'
        file_keys[f'{task_dict["intermediate_file_name"]}_1.maple'] = 1
        with open(intermediate_filename, 'a') as f_:
            f_.write('%s\t%s\n' % (1, dt))
        
        return word_keys, file_keys

    def t2_m_run_maple_executable(self, line, task_dict, word_keys = {}, file_keys={}):

        target_col_idx = self.get_target_col_idx(task_dict["extra_info"]["target_col"])
        target_col_val = task_dict["extra_info"]["target_val"]
        
        row = line.strip().split(',')
        col_val = row[target_col_idx]

        if col_val == target_col_val:
            row_id = str(row[0])
            intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_{row_id}.maple'
            file_keys[f'{task_dict["intermediate_file_name"]}_{row_id}.maple'] = 1
            word_keys[line] = 1
            with open(intermediate_filename, 'a') as f_:
                f_.write(f'{[line, 1]}\n')
        
        return word_keys, file_keys

    def t2_moa_run_maple_executable(self,  line, task_dict, word_keys = {}, file_keys={}):
        row = line.strip().split(',')

        row_id = str(row[0])
        intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_{row_id}.maple'
        file_keys[f'{task_dict["intermediate_file_name"]}_{row_id}.maple'] = 1
        word_keys[line] = 1
        with open(intermediate_filename, 'a') as f_:
            f_.write(f'{[line, 1]}\n')
        
        return word_keys, file_keys

    def t2_mreg_run_maple_executable(self, line, task_dict, word_keys={}, file_keys={}):
        row = line.strip().split(',')
        regex_str = task_dict["extra_info"]["regex_str"]

        ans = re.search(regex_str, line) is not None
        if ans:
            # row_id = str(row[0])
            intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_1.maple'
            file_keys[f'{task_dict["intermediate_file_name"]}_1.maple'] = 1
            word_keys[1] = 1
            with open(intermediate_filename, 'a') as f_:
                f_.write('%s\t%s\n' % (1, line.strip()))
        
        return word_keys, file_keys

    def get_file_locally(self, filename, local_file_location, get_file_location, vm_id = "fa23-cs425-8201.cs.illinois.edu"):
        usr = "sb82"
        ip = socket.gethostbyname(vm_id)
        scp = ["scp", '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'{usr}@{ip}:{"/home/"+usr+f"/mp4_82/{get_file_location}/"+filename}', f"{'/home/'+usr+f'/mp4_82/{local_file_location}/'+filename}"]
        scp_run = subprocess.run(scp, check=True)
        return 
    
    def mjoin_run_maple_executable(self, line, task_dict, word_keys, file_keys, idx):
        
        col_idx = "col"+str(idx + 1)
        col_name = task_dict["extra_info"][col_idx]

        row = line.strip().split(",")
        use_2 = False
        if idx == 1:
            use_2= True
        val_idx = self.get_target_col_idx(col_name, use_2 = use_2)
        val = row[val_idx]
        sanitized_v = row[val_idx]
        
        if "/" in val:
            sanitized_v = val.replace("/", "_")
        
        if val.strip() == '':
            sanitized_v = str(val.count(" ")) + "_numspace_str"
        sanitized_v = sanitized_v.replace(" ", "space")
        
        # print(val , sanitized_v , line)
        intermediate_filename = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_{sanitized_v}.maple'
        
        word_keys[val] = 1
        
        file_keys[f'{task_dict["intermediate_file_name"]}_{sanitized_v}.maple'] = 1
        
        with open(intermediate_filename, 'a') as f_:
            f_.write('%s\t%s\n' % (sanitized_v, (idx, line)))
        
        return word_keys, file_keys        

    def execute_rcvd_maple_cmd(self, task_dict):
        '''
        Run only on clients
        '''
        
        word_keys = {}
        file_keys = {}
        for i in range(len(task_dict["filename_list"])):
            f = task_dict["filename_list"][i]

            local_file_location = "sdfs"

            if task_dict["get_file_location"] != "sdfs" or os.path.exists(f"/home/sb82/mp4_82/sdfs/" + f) == False:
                local_file_location = "temp_maple_files"
                self.get_file_locally(f, local_file_location, task_dict["get_file_location"])

            print(f"Running for file {f}")

            lines_range = [((j-1) * task_dict["lines_per_shard"][i] + 1, j * task_dict["lines_per_shard"][i]) for j in task_dict["shard_list"]]
            fp = f"/home/sb82/mp4_82/{local_file_location}/" + f
            # if task_dict["mj_exe"] == "t2_mjreg":
            #     kl = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_1.maple'
            #     al = 0
            #     if os.path.exists(kl):
            #         al = subprocess.run(['wc', '-l', kl], stdout=subprocess.PIPE, universal_newlines=True)
            #         al = int(al.stdout.split()[0])
            #     task_dict["default_start_point"] = al
            #     print(task_dict["default_start_point"])
            
            print(f"lines range is : {lines_range}")
            for t in lines_range:
                line_number = 0
                                
                with open(fp, 'r') as file:
                    for line in file:
                        line_number += 1
                        if t[0] <= line_number <= t[1]:
                            if task_dict["mj_exe"] == "wc":
                                word_keys, file_keys = self.wc_run_maple_executable(line, task_dict, word_keys, file_keys)
                            elif task_dict["mj_exe"] == "t1_mj1":
                                if line_number == 1:
                                    continue
                                word_keys, file_keys = self.t1_m1_run_maple_executable(line, task_dict, word_keys, file_keys)
                            elif task_dict["mj_exe"] == "t1_mj2":
                                word_keys, file_keys = self.t1_m2_run_maple_executable(line, task_dict, word_keys, file_keys)
                            elif task_dict["mj_exe"] == "t2_mj":
                                word_keys, file_keys = self.t2_m_run_maple_executable(line, task_dict, word_keys, file_keys)
                            elif task_dict["mj_exe"] == "t2_mjoa":
                                word_keys, file_keys = self.t2_moa_run_maple_executable(line, task_dict, word_keys, file_keys)
                            elif task_dict["mj_exe"] == "t2_mjreg":
                                word_keys, file_keys = self.t2_mreg_run_maple_executable(line, task_dict, word_keys, file_keys)
                                
                            elif task_dict["mj_exe"] == "mj_join":
                                word_keys, file_keys = self.mjoin_run_maple_executable(line, task_dict, word_keys, file_keys, idx = i)
                        
        # if task_dict["mj_exe"] == "t2_mjreg":
        #     kl = f'/home/sb82/mp4_82/maple_files/{task_dict["intermediate_file_name"]}_1.maple'
        #     al = 0
        #     if os.path.exists(kl):
        #         al = subprocess.run(['wc', '-l', kl], stdout=subprocess.PIPE, universal_newlines=True)
        #         al = int(al.stdout.split()[0])
        #     task_dict["default_end_point"] = al
        #     print("yyyyyyy")
        #     print(al)
        

        self.multicast_messages([("maple_completed", word_keys, self.leader)], d2 = {self.host_name : 1, "task_dict": task_dict, "file_keys" : file_keys})

        # print("Rcvd dict")
        # print(task_dict)

        #add code to process received maple_cmds