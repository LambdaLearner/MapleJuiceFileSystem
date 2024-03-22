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

#TODO list
'''
code cleanup , repo cleanup
adding ack code
report data
'''
#INITIAL DEMO TEST WORKING

from fd4 import *
from fmj4 import *

class FileManager:
    def __init__(self):
        self.file_map = {} #holds which file is in which vm, k = file_name, v = dict of vms
        self.machine_map = {} #holds which machine has what file, k = vm, v = dict of file_names
        self.leader = "fa23-cs425-8201.cs.illinois.edu"
        self.FileManagerLock = threading.RLock()
        self.logger = setup_logger(self.__class__.__name__, socket.gethostname())
        self.logger.info("File manager class is initialized")

    def add_file(self, sdfsfilename, server_id):
        
        self.logger.debug(f"Received add_file request: filename : {sdfsfilename}, server_id : {server_id}")
        if sdfsfilename not in self.file_map.keys():
            self.file_map[sdfsfilename] = {}
            
        self.file_map[sdfsfilename][server_id] = 1

        if server_id not in self.machine_map.keys():
            self.machine_map[server_id] = {}

        self.machine_map[server_id][sdfsfilename] = 1
        self.logger.debug(f"File_map after operation is : {self.file_map}")
        self.logger.debug(f"Machine_map after operation is : {self.machine_map}")

    def delete_file(self, sdfsfilename, server_id):
        if sdfsfilename in self.file_map.keys() and self.file_map[sdfsfilename].get(server_id) == 1:
            self.logger.debug(f"Executing delete. Input rcvd: sdfsfilename : {sdfsfilename}, server_id : {server_id}")
            self.file_map[sdfsfilename].pop(server_id)
            self.machine_map[server_id].pop(sdfsfilename)


class SDFS:
    def __init__(self, my_ip, my_port, host_name):
        self.FileManager = FileManager()
        self.ip = my_ip
        self.my_port = 5001
        self.host_name = host_name
        self.sock = None
        self.setup_leader_socket()
        self.read_dict = {}
        self.write_dict = {}
        self.read_queue = deque()
        self.write_queue = deque()
        self.fd = failure_detection_class(host_name, my_ip, 5000, time.time())
        self.data_transfer_lock = threading.RLock()
        self.logger = setup_logger(self.__class__.__name__, self.host_name)
        self.logger.info("SDFS class is initialized")
        self.leader_election = False
        self.leader = "fa23-cs425-8201.cs.illinois.edu"
        self.mj = MichaelJackson(self,self.fd, self.FileManager)


    def setup_leader_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.my_port))
        self.sock.listen(1000)
    # add failure detection here


    def check_for_starvation(self, inp_dict):
        l = list(inp_dict.keys())
        for i in l:
            v = inp_dict.get(i, -1) 
            if v == 4:
                return True
        return False
    
    def data_transfer_helper(self):
        '''
        For reads and writes: if there are 4 writes already queue a read and vice versa.
        Read operation: send all the replicas from which the file can be read by thte requester
        Write operation: send all replicas to write the file to.
        '''

        while(True):
            with self.data_transfer_lock:
                is_writes_starving = self.check_for_starvation(self.write_dict)
                is_reads_starving = self.check_for_starvation(self.read_dict)
                sdfsfilename = ""
                if len(self.read_queue) != 0 and is_writes_starving == False:
                    #execute_read_op

                    reply_message = "Allow get"
                    
                    #when done pop elements from queue
                    k = self.read_queue.popleft()
                    # get the sdfsfilename
                    sdfsfilename = self.read_dict[k][2]

                    replica_vm_list = self.FileManager.file_map.get(sdfsfilename, {})
                    pickled_data = pickle.dumps((reply_message, list(replica_vm_list.keys())))
                    self.logger.info(f"Starting read operation for file : {sdfsfilename} , replicas_list : {replica_vm_list}")

                    self.read_dict[k][0].sendall(pickled_data)

                    resp = self.read_dict[k][0].recv(5000)
                    msg = resp.decode('utf-8')

                    if msg == "get_acknowledgement":
                        self.logger.info(f"Read operation completed for file : {sdfsfilename}.")
                        self.read_dict.pop(k)
                    else:
                        continue

                    for i in self.write_dict.keys():
                        self.write_dict[i] = (self.write_dict[i][0], self.write_dict[i][1] + 1)            

                if len(self.write_queue) != 0 and is_reads_starving == False:
                    k = self.write_queue.popleft()
                    reply_message = "Allow write"
                    replica_vm_list = self.FileManager.file_map.get(self.write_dict[k][3], [])
                    if len(replica_vm_list) == 0:
                        replica_vm_list = self.get_replica_server_id(self.write_dict[k][3])
                    pickled_data = pickle.dumps((reply_message, replica_vm_list))

                    self.logger.info(f"Starting write operation. replica_list : {replica_vm_list}")

                    # send from clientsocket
                    self.write_dict[k][0].sendall(pickled_data)
                    # receive ack from the client socket
                    resp = self.write_dict[k][0].recv(5000)
                    msg = resp.decode('utf-8')
                    if msg in ["put_acknowledgement" , "del_acknowledgement"]:
                        self.logger.info(f"Write operation completed.")
                        self.write_dict.pop(k)
                    else:
                        continue
                    
                    for i in self.read_dict.keys():
                        self.read_dict[i] = (self.read_dict[i][0], self.read_dict[i][1] + 1)         

            
    def data_transfer(self):
        '''
        Queues the incoming put/get requests into appropriate queues and dicts.
        '''
        while True:
            client_socket, addr = self.sock.accept()
            response_data = client_socket.recv(50000)
            message = response_data.decode('utf-8')

            # read_dict["addr"] = (client_obj, 0) -> 0 is number of writes this read op has seen
            # write_dict["addr"] = (client_socket_obj, 0) -> 0 is number of reads this write op has seen
            # check for starving read ops
            # if any -> run read ops
            # check for starving write ops
            # if any -> run write ops
            
            if self.host_name == self.FileManager.leader and ("put_request" in message or "del_request" in message):
                message_list = message.split(" ")
                k1 = addr[0] + "_" + str(time.time())
                # with self.data_transfer_lock:
                self.logger.info(f"Queuing write request for file : {message_list[2]}. Current W queue size : {len(self.write_queue)}")
                self.write_dict[k1] = (client_socket, 0, message_list[1], message_list[2])
                self.write_queue.append(k1)
                    
            elif self.host_name == self.FileManager.leader and "getfrom" in message:

                message_list = message.split(" ")

                k1 = addr[0] + "_" + str(time.time())
                # with self.data_transfer_lock:
                self.logger.info(f"Queuing read request for file : {message_list[1]}. Current R queue size : {len(self.read_queue)}")
                self.read_dict[k1] = (client_socket, 0, message_list[1])
                self.read_queue.append(k1)
            
            elif self.host_name == self.FileManager.leader and ("maple_req" in message or "juice_req" in message):
                with self.mj.dict_lock:
                    k1 = addr[0] + "_" + str(time.time())
                    self.mj.mj_dict[k1] = (message, {})
                    self.logger.info(f"Queuing MJ request. Current MJ queue size : {len(self.mj.mj_queue)}")
                    self.mj.mj_queue.append(k1)
            
            elif self.host_name == self.FileManager.leader and ("sql_req" in message):
                with self.mj.sql_dict_lock:
                    k1 = addr[0] + "_" + str(time.time())
                    self.mj.sql_dict[k1] = message
                    self.logger.info(f"Queuing SQL request. Current SQL queue size : {len(self.mj.sql_queue)}")
                    self.mj.sql_queue.append(k1)
            


    
    def get_replica_server_id(self, sdfsfilename):
        '''
        Hashes the file name to get primary node to store the file, along with its replicas
        '''
        # file_name_sum = sum(ord(c) for c in sdfsfilename)
        # host_str = "fa23-cs425-82{XX}.cs.illinois.edu"
        # primary_vm_id = (file_name_sum % 10 + 1)
        # replica_vm_list = []
        # for i in range(-1,3):
        #     vm_id = primary_vm_id + i
        #     if vm_id < 1:
        #         vm_id = vm_id + 10
        #     if vm_id > 10:
        #         vm_id = vm_id % 10
        #     if vm_id >=1 and vm_id < 10:
        #         vm_id = "0" + str(vm_id)
        #     else:
        #         vm_id = str(vm_id)
            

        #     replica_vm_list.append(host_str.format(XX = vm_id))
        # return replica_vm_list
        
        #TODO: choose 4 random nodes in the membership list as the replicas.
    
        with self.fd.lock:
            replicas = list(set(list(self.fd.membership_list.keys())) - set([self.leader]))
            random.shuffle(replicas)  # Shuffle the list in-place
            l = replicas[:3]
            return list(set(l) | set([self.leader]))

    # def scp(self, file, destination):
    #     subprocess.run([["scp", file, destination]])

    def execute_put(self, file_addr, usr, sdfsfilename, local_file_name, mj_op, directory= "local_files"):

        ip = socket.gethostbyname(file_addr)
        if mj_op == True:
            scp = ["scp",'-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'/home/{usr}/mp4_82/juice_files/{local_file_name}', f'{usr}@{ip}:/home/{usr}/mp4_82/sdfs/{sdfsfilename}']
        else:    
            scp = ["scp",'-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'/home/{usr}/mp4_82/{directory}/{local_file_name}', f'{usr}@{ip}:/home/{usr}/mp4_82/sdfs/{sdfsfilename}']
        

        self.logger.debug(f"Running scp. Command list : {scp}")

        scp_run = subprocess.run(scp, check=True)
        with self.FileManager.FileManagerLock:
            self.FileManager.add_file(sdfsfilename, file_addr)
        
        return scp_run.returncode

    def multicast_machine_file_map_changes(self, msg):

        UDP_PORT = 1234
        with self.FileManager.FileManagerLock:
            pickled_data = pickle.dumps((msg, self.FileManager.file_map, self.FileManager.machine_map))
                
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            with self.fd.lock:
                # self.logger.info(f"Multicasting to {list(self.fd.membership_list.keys())} , port : 1234")
                for x in self.fd.membership_list.keys():
                    if self.fd.membership_list[x] == self.host_name:
                        continue
                    sock.sendto(pickled_data, (self.fd.membership_list[x].ip, UDP_PORT))

    def mj_operation(self, command, operation, param = 'no'):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_leader:
            leader_ip = socket.gethostbyname(self.leader)       
            port = 5001 
            socket_leader.connect((leader_ip, port))
            socket_leader.sendall((operation + "_req " + param + " " + command ).encode('utf-8'))

    def put_file_operation(self, local_file_name, sdfsfilename, mj_op = False, directory="local_files"):
        '''
        it should send the put command to the leader -> asking if it is okay to perform the write operation.
        If leader says ok, then you go ahead and transfer the file.
        question : can you transfer the file to 4 replica machines using a for loop?
        '''
        usr = "sb82"
        st = time.time()
        # closes socket when needed
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_leader:
            leader_ip = socket.gethostbyname(self.FileManager.leader)
            port = 5001
            # self.logger.debug(f"Sending put request to leader: {self.FileManager.leader}")

            socket_leader.connect((leader_ip, port))
            socket_leader.sendall(("put_request: " + local_file_name + " " + sdfsfilename).encode('utf-8'))
            self.logger.debug(f"Put request sent to leader: {self.FileManager.leader}")

            leader_response = socket_leader.recv(4096)
            leader_message, replica_vms_list = pickle.loads(leader_response)
            if leader_message == "Allow write" and replica_vms_list:
                
                with ThreadPoolExecutor() as executor:
                    results = executor.map(lambda x: self.execute_put(x, usr, sdfsfilename, local_file_name, mj_op, directory), replica_vms_list)
                # TODO: add ip address?
                msg = "Put_Completed_Successfully"
                self.multicast_machine_file_map_changes(msg)
                socket_leader.sendall("put_acknowledgement".encode('utf-8'))
        en = time.time()
        self.logger.info(f"Time taken to complete Put request: {(en-st) * 1000}")

    def execute_del(self, file_addr, usr, sdfsfilename, local_file_name):

        ip = socket.gethostbyname(file_addr)
        scp = ["ssh",'-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'{usr}@{ip}', f'rm /home/{usr}/mp4_82/sdfs/{sdfsfilename}']
        

        self.logger.debug(f"Running rm. Command list : {scp}")

        scp_run = subprocess.run(scp, check=True)
        with self.FileManager.FileManagerLock:
            self.FileManager.delete_file(sdfsfilename, file_addr)
        
        return scp_run.returncode

    def del_file_operation(self, local_file_name, sdfsfilename):
        '''
        it should send the put command to the leader -> asking if it is okay to perform the write operation.
        If leader says ok, then you go ahead and transfer the file.
        question : can you transfer the file to 4 replica machines using a for loop?
        '''
        usr = "sb82"
        st = time.time()
        # closes socket when needed
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_leader:
            leader_ip = socket.gethostbyname(self.FileManager.leader)
            port = 5001
            # self.logger.debug(f"Sending put request to leader: {self.FileManager.leader}")

            socket_leader.connect((leader_ip, port))
            socket_leader.sendall(("del_request: " + local_file_name + " " + sdfsfilename).encode('utf-8'))
            self.logger.debug(f"Del request sent to leader: {self.FileManager.leader}")

            leader_response = socket_leader.recv(4096)
            leader_message, replica_vms_list = pickle.loads(leader_response)
            if leader_message == "Allow write" and replica_vms_list:
                
                with ThreadPoolExecutor() as executor:
                    results = executor.map(lambda x: self.execute_del(x, usr, sdfsfilename, local_file_name), replica_vms_list)
                # TODO: add ip address?
                msg = "Del_Completed_Successfully"
                self.multicast_machine_file_map_changes(msg)
                socket_leader.sendall("del_acknowledgement".encode('utf-8'))
        en = time.time()
        self.logger.info(f"Time taken to complete DEL request: {(en-st) * 1000}")


    def update_mappings_locally(self, file_map_updated, machine_map_updated, message):
        '''
        Updating the file_map and machine_map in FileManager with the recevied mapping.
        Assuming the rcvd mappings are the latest.
        Replication in case of failures needs to be handled separately.
        '''

        with self.FileManager.FileManagerLock:
            for filename in file_map_updated.keys():
                # if filename in self.FileManager.file_map.keys():
                #     self.FileManager.file_map[filename] = file_map_updated[filename]
                # else:
                self.FileManager.file_map[filename] = file_map_updated[filename]
        
            for vm in machine_map_updated.keys():
                # if machine_map_updated[vm] == self.FileManager.machine_map.get(vm, {}):
                #     continue
                # else:
                if message == "Put_Completed_Successfully":
                    new_val = list(set(list(self.FileManager.machine_map.get(vm, {}).keys())) | set(list(machine_map_updated[vm].keys())))
                    
                    self.FileManager.machine_map[vm] = dict(zip(new_val, [1] * len(new_val)))
                elif message in ["Updated_File_Map_After_Failure", "Del_Completed_Successfully"]:
                    self.FileManager.machine_map[vm] = machine_map_updated[vm] 
                
    def delete_mj_files(self, d1):

        directory1 = "/home/sb82/mp4_82/sdfs/"
        directory2 = "/home/sb82/mp4_82/maple_files/"
        directory3 = "/home/sb82/mp4_82/juice_files/"
        directory4 = "/home/sb82/mp4_82/temp_maple_files/"
        
        prefix = d1["intermediate_file_name"]


        command1 = f"rm {directory1}/{prefix}*"
        command2 = f"rm {directory2}/{prefix}*"
        command3 = f"rm {directory3}/{prefix}*"
        command4 = f"rm {directory4}/{prefix}*"

        process = subprocess.run(command1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process = subprocess.run(command2, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process = subprocess.run(command3, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process = subprocess.run(command4, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


        with self.FileManager.FileManagerLock:

            to_delete = [k for k in self.FileManager.file_map.keys() if k[:len(prefix)] == prefix]

            for i in to_delete:
                self.FileManager.file_map.pop(i, [])
            for k in self.FileManager.machine_map.keys():
                new_files = list( set(list(self.FileManager.machine_map[k].keys())) - set(to_delete))
                d = {}
                if len(new_files):
                    d = dict(zip(new_files , [1] * len(new_files)))
                self.FileManager.machine_map[k] = d       

    def udp_listener(self):
        UDP_IP = self.ip
        UDP_PORT = 1234

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((UDP_IP, UDP_PORT))
        
        while True:    
            message = ""
            received_list = {}

            response_data = sock.recv(50000)

            if len(response_data) > 0:
                message, d1, d2 = pickle.loads(response_data)
            
            self.logger.info(f"Received UDP data on Port 1234. Message : {message}")
            
            if message in ["maple_completed"]:
                self.mj.queue_rcvd_maple_complete_message(d1, d2)

            if message in ["juice_completed"]:
                self.mj.process_juice_complete_message(d1, d2)

            if message in ["Run_Maple"]:
                self.mj.execute_rcvd_maple_cmd(d1)
            
            if message in ["Run_Juice"]:
                self.mj.execute_rcvd_juice_cmd(d1)
            
            if message in ["Delete_Maple_Files"]:
                self.delete_mj_files(d1)

            if message in ["Put_Completed_Successfully", "Updated_File_Map_After_Failure", "Del_Completed_Successfully"] :
                self.update_mappings_locally(d1, d2, message)
            
    
    def get_file_operation(self, sdfsfilename, localfilename, multiread_vms = []):

        '''
        1. Get a file from the sdfs into local.
        2. ie contact the leader? or contact the primary vm directly.
        Contact the leader, and then get the primary vm only or list of all replicas?.
        Contact this vm directly and retrieve the file into the local.
        3. Also execute a multi read
        '''
        # users = sb82, grao5?
        usr = "sb82"
        st = time.time()
        # closes socket when needed
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_leader:

            leader_ip = socket.gethostbyname(self.FileManager.leader)
            port = 5001
            socket_leader.connect((leader_ip, port))
            socket_leader.sendall(("getfrom: " + sdfsfilename).encode('utf-8'))
            self.logger.debug(f"Get request sent to leader: {self.FileManager.leader}")

            # we want to get the dict of all the keys (primary, replicas) having the sdfs file name. 
            # then contact the primary then replicas one by one until data is successfully rcvd. 
            # ie only one vm needs to send the file succesfully.
            leader_response = socket_leader.recv(4096)
            leader_message, received_vms = pickle.loads(leader_response)

            if leader_message == "Allow get" and received_vms:
                got_file = 1
                member_name_list = list(self.fd.membership_list.keys())
                print(f"===== {member_name_list} , {type(member_name_list)}")
                print(f"===== {received_vms} , {type(received_vms)}")

                alive_replicas = set(member_name_list).intersection ( set(received_vms) )
                print("alive replicas:")
                print(alive_replicas)
                print("member_name_list:")
                print(member_name_list)
                for file_addr in alive_replicas:
                    # need to check how to convert sdfsfilename into localfilename at the replica vm through scp.
                    # another way is to have a mapping of their sdfsfilenames and loacls? : yes we can maintain that map as well!
                    destination_file = sdfsfilename
                    # check -> if it is true then break from t he loop.
                    print(file_addr)
                    print(socket.gethostbyname(file_addr))
                    # if its a multiread
                    if len(multiread_vms) > 0:
                        scp_done = 0
                        for vm_id in multiread_vms:
                            vm_ip_addr = str(vm_id) 
                            if int(vm_id) < 10:
                                vm_ip_addr = "0" + vm_ip_addr
                            host_str = "fa23-cs425-82{XX}.cs.illinois.edu"
                            vm_ip_addr = host_str.format(XX = vm_ip_addr)
                           

                            # scp = ["scp", f'{usr}@{socket.gethostbyname(file_addr)}:{"mp4_82/sdfs/"+destination_file}', localfilename]
                            scp = ["scp", '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'{usr}@{socket.gethostbyname(file_addr)}:{"/home/"+usr+"/mp4_82/sdfs/"+sdfsfilename}', f'{usr}@{socket.gethostbyname(vm_ip_addr)}:{"/home/"+usr+"/mp4_82/get_file_ops/"+localfilename}']
                            scp_run = subprocess.run(scp, check=True)
                            scp_done += scp_run.returncode
                        return scp_done
                            
                    scp = ["scp", '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'{usr}@{socket.gethostbyname(file_addr)}:{"/home/"+usr+"/mp4_82/sdfs/"+destination_file}', f"{'/home/'+usr+'/mp4_82/get_file_ops/'+localfilename}"]
                    scp_run = subprocess.run(scp, check=True)
                    got_file = scp_run.returncode
                    if got_file == 0:
                        socket_leader.sendall("get_acknowledgement".encode('utf-8'))
                        self.logger.debug(f"Get request comepleted")

                        break
                if got_file == 1:
                    self.logger.info(f"Get request failed")
        en = time.time()
        self.logger.info(f"Time taken to complete Get request: {(en-st) * 1000}")
                
    def run_ls_command(self, file_name):

        with self.FileManager.FileManagerLock:
            files = self.FileManager.file_map.get(file_name, {}).keys()
            self.logger.info(f"The given file : {file_name} is stored on : {files}")

            # print(f"")
        

    def run_store_command(self):

        with self.FileManager.FileManagerLock:
            files = self.FileManager.machine_map.get(socket.gethostname(), {}).keys()
            self.logger.info(f"Files stored on this VM are : {files}")

            # print(f"")


    def scp_replication(self, file_name, source, destination):
        usr = "sb82"
        # for key, value in file_vm_map.items():

        # self.logger.debug(f"Replication process. filename is {file_name}, source is {source}, destination is {destination}")
        ip_source = socket.gethostbyname(source)
        ip_dest = socket.gethostbyname(destination)
        # self.logger.debug(f"Replication process. filename is {file_name}, key is {key}, source is {ip_source}, destination is {value[1]}")
        
        # print("scp command to be executed is ::::")
        scp = ["scp", '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', f'{usr}@{ip_source}:{"/home/"+usr+"/mp4_82/sdfs/"+file_name}', f'{usr}@{ip_dest}:{"/home/"+ usr +"/mp4_82/sdfs/"+file_name}']
        # self.logger.debug(f"Starting scp replication")
        
        # self.logger.debug(f"Running scp. Command list : {scp}")
        # print(scp)
        scp_run = subprocess.run(scp, check=True)
        
        return scp_run.returncode


    def detect_failed_replica_and_leader(self):

        '''
        1. Handle replica failure. (Only done by the leader)
        2. Handle leader failure: We assume leader = introducer at all times.
        '''
        while(True):
            time.sleep(6) # > sum of tfail and tcleanup -> because we want to be sure that nodes are failed
            # TODO: add leader election testing.
            # check for leadership position. Is the current node not a leader?
            if self.host_name != "fa23-cs425-8201.cs.illinois.edu" and self.leader_election:
                continue
            
                # Run failure detection every 4-5 seconds.
            # There are some false positives from the failure detection from mp2, so they also get re-replicated
            
            # get all the hostnames currently alive
            # with server.rlock:
            #     membership_list_names = [socket.gethostbyaddr(server.MembershipList[m]["addr"][0])[0] for m in server.MembershipList.keys()]
            # Get the set of nodes storing files in the sdfs system that have failed.

            flag = False
            eligible_vms_dict = {}
            en = 0
            st = 0
            self.FileManager.FileManagerLock.acquire()
            self.fd.lock.acquire()
            membership_list_names = list(self.fd.membership_list.keys())
            failed_nodes = set(list(self.FileManager.machine_map.keys())) - set(membership_list_names)
            # Re-replication
            if len(failed_nodes) > 0:
                flag = True
                self.logger.info(f"Failed Node detected: {failed_nodes}")
                st = time.time()
                #eligible_vms_dict = {}
                # vm:  hostnames of the failed nodes
                for vm in failed_nodes:

                    files_in_vm = list(self.FileManager.machine_map[vm].keys())
                    self.logger.info(f"Files held by failed node {vm} : {files_in_vm}")
                    eligible_vms_dict[vm] = {}
                        # format, k = file_name, v = (live_node, new_node)
                    for f in files_in_vm:
                        # get one alive replica
                        live_node_holding_file = list((set(list(self.FileManager.file_map[f].keys())) - failed_nodes))[0]
                        # get another alive vm that isn't already a vm
                        eligible_vm_for_transfer = list(set(membership_list_names) - set(list(self.FileManager.file_map[f].keys())))
                        if len(eligible_vm_for_transfer) == 0:
                            self.logger.info(f"All Files in system already hold the file")
                            continue
                        else:
                            eligible_vm_for_transfer = eligible_vm_for_transfer[0]
                        # add mapping of the file to the new eligible vm and delete the failed vm
                        eligible_vms_dict[vm][f] = (live_node_holding_file, eligible_vm_for_transfer)
                        self.FileManager.add_file(f, eligible_vm_for_transfer)
                        self.FileManager.delete_file(f, vm)
                    self.logger.debug(f"Re Replication file map log : {eligible_vms_dict}")
                    self.fd.lock.release()
                    with ThreadPoolExecutor(max_workers = 2) as executor:
                        results = executor.map(lambda x: self.scp_replication(x, eligible_vms_dict[vm][x][0], eligible_vms_dict[vm][x][1]), list(eligible_vms_dict[vm].keys()))
                    self.fd.lock.acquire()
                    self.FileManager.machine_map.pop(vm)
            
            self.fd.lock.release()
            self.FileManager.FileManagerLock.release()
            if flag:
                self.multicast_machine_file_map_changes(msg = "Updated_File_Map_After_Failure")
                en = time.time()
                self.logger.info(f"Re Replication time in system : {(en-st)*1000}")


def cli(file_system_node):
    while True:
        '''
        Commands to be added as given in demo spec : 
        1. put (used to insert and update file), 
        2. get (fetch file to local directory), 
        3. delete (delete file)
        4. ls sdfsfilename (list all machines which contain the file)
        5. store (list all files stored at the vm)
        
        '''
        command = input("Enter command (): ")
        command_list = command.split(" ")
        if command_list[0] in ("put", "get"):
            if len(command_list) < 3:
                print("incorrect cmd len")
                continue
        
        elif command_list[0] in ("delete", "ls"):
            if len(command_list) < 2:
                print("incorrect cmd len")
                continue

        if command_list[0] == "put":
            file_system_node.put_file_operation(command_list[1], command_list[2]) #Write operation
            print("Added file to machines!")
        elif command_list[0] == "get":
            file_system_node.get_file_operation(command_list[1], command_list[2]) #Read operation
            print("Get operation complete!")
        elif command_list[0] == "delete": #Write operation?
            file_system_node.del_file_operation(local_file_name = "NOFILE", sdfsfilename=command_list[1])
            print("Delete operation complete!")
        elif command_list[0] == "ls":
            file_system_node.run_ls_command(command_list[1]) # use file map for this
            print("ls operation complete!")
        elif command_list[0] == "store":
            file_system_node.run_store_command() # use machine_map for this
            print("store operation complete!")
        elif command_list[0] == "multiread":
            return_code = file_system_node.get_file_operation(command_list[1], command_list[2], command_list[3:])
            if return_code == 0:
                print("Multiread completed")
            else:
                print("Multiread failed")

        elif command == "leave":
            file_system_node.fd.leave_group()
            print("Left the group.")
        elif command == "join":
            file_system_node.fd.join(file_system_node)
            #TODO: add code for this
        elif command == "list_mem":
            file_system_node.fd.print_membership_list()
        elif command == "list_self":
            file_system_node.fd.print_self_id()
        elif command == "enable_suspicion":
            file_system_node.fd.switch_gossip_protocol("Gossip+S")
            #TODO: add code for this
        elif command == "disable_suspicion":
            file_system_node.fd.switch_gossip_protocol("Gossip")
            #TODO: add code for this
        elif command_list[0] == "maple":
            param = input("Any last wish before you run maple (): ")
            file_system_node.mj_operation(command = command, operation ="maple", param=param)
        elif command_list[0] == "juice":
            file_system_node.mj_operation(command = command, operation ="juice")
        elif command_list[0] == "normal_test":
            command1 = "maple t1_mj1 4 d1 s7.s"
            file_system_node.mj_operation(command = command1, operation ="maple")
            command2 = "juice t1_mj1 4 d1 s8.s 1"
            file_system_node.mj_operation(command = command2, operation ="juice")

            command1 = "maple t1_mj2 4 d2 s8.s"
            file_system_node.mj_operation(command = command1, operation ="maple")
            command2 = "juice t1_mj2 4 d2 s9.s 1"
            file_system_node.mj_operation(command = command2, operation ="juice")
            
        elif command_list[0] == "test1":
            param = command_list[1]
            command1 = "maple t1_mj1 4 d1 s7.s"
            file_system_node.mj_operation(command = command1, operation ="maple", param = param)
            command2 = "juice t1_mj1 4 d1 s8.s 1"
            file_system_node.mj_operation(command = command2, operation ="juice")

            command1 = "maple t1_mj2 4 d2 s8.s"
            file_system_node.mj_operation(command = command1, operation ="maple")
            command2 = "juice t1_mj2 4 d2 s9.s 1"
            file_system_node.mj_operation(command = command2, operation ="juice")
        elif command_list[0] == "sql_test":
            command1 = "sql select all FROM s1.s WHERE ( UPS=N AND ( OBJECTID=41 OR Detection_=Video ) ) -- NORMAL"
            file_system_node.mj_operation(command = command1, operation ="sql")
        
        elif command_list[0] == "report_join":
            command1 = "sql select all FROM s1 JOIN s2 WHERE ( s1.Weather_Condition=s2.Weather ) -- JOIN"
            file_system_node.mj_operation(command = command1, operation ="sql")
        
        elif command_list[0] == "sql_test_reg":
            command1 = "sql select all FROM s1.s WHERE ( Video.*Radio ) -- REGEX"
            file_system_node.mj_operation(command = command1, operation ="sql")
        
        elif command_list[0] == "report_reg":
            command1 = "sql select all FROM s1.s WHERE ( \d{4}-11-\d{2}#s17:\d{2}:\d{2},\d{4}-11-\d{2}#s17:\d{2}:\d{2} ) -- REGEX"
            file_system_node.mj_operation(command = command1, operation ="sql")
        
        elif command_list[0] == "sql_join":
            command1 = "sql select all FROM s1 JOIN s2 WHERE ( s1.Ownership=s2.City ) -- JOIN"
            file_system_node.mj_operation(command = command1, operation ="sql")
        
        elif command_list[0] == "sql":
            file_system_node.mj_operation(command = command, operation ="sql")
        
        else:
            print("Invalid operation! 1 chance left!")        


if __name__ == '__main__':
    
    # Main method

    introducer_hostname = "fa23-cs425-8201.cs.illinois.edu"
    introducer_ip = socket.gethostbyname(introducer_hostname)
    
    my_ip = socket.gethostbyname(socket.gethostname())   
    my_port = 5000
    host_name = socket.gethostname()
    sdfs_node = SDFS(my_ip, 5001, host_name)
    

    if my_ip == introducer_ip:
        threading.Thread(target=sdfs_node.fd.handle_join_requests, args=(sdfs_node, )).start()
    
    with sdfs_node.fd.lock:
        sdfs_node.fd.membership_list[host_name] = sdfs_node.fd.node  # Adding current node to the membership list

    # heartbeating and listening threads
    threading.Thread(target=sdfs_node.fd.heartbeat, args=()).start()
    threading.Thread(target=sdfs_node.fd.heartbeat_with_suspicion, args=()).start()
    
    threading.Thread(target=sdfs_node.fd.listen_toss, args = ()).start()
    threading.Thread(target=sdfs_node.fd.failure_detection, args = ()).start()
    # Bandwidth thread
    # threading.Thread(target=log_bandwidth, args=()).start()

    

    # CLI thread
    threading.Thread(target=cli, args=(sdfs_node, )).start()

    # listening thread
    if my_ip == introducer_ip:
        threading.Thread(target=sdfs_node.data_transfer, args=()).start()

    if my_ip == introducer_ip:
        threading.Thread(target=sdfs_node.mj.queue_manager, args=()).start()

        threading.Thread(target=sdfs_node.mj.sql_queue_manager, args=()).start()

    # listening helper thread
    if my_ip == introducer_ip:
        threading.Thread(target=sdfs_node.data_transfer_helper, args=()).start()

    # listening to multicasts
    threading.Thread(target=sdfs_node.udp_listener, args=()).start()

    # detect failures for re-replication and leader election
    if my_ip == introducer_ip:
        threading.Thread(target=sdfs_node.detect_failed_replica_and_leader, args=()).start()
    