package wso_project;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.UtilizationModelStochastic;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class WsoExperiment {
	
	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<Vm> vmlist;
	
	public static long seed;
	
	@SuppressWarnings("unused")
	public static void main(String[] args) {

		WsoExperiment.seed = 100;
		Log.printLine("Starting WSO Experiment...");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			// Third step: Create Broker
			DatacenterBrokerWso broker = createBroker();
			int brokerId = broker.getId();

			// Fourth step: Create virtual machines
			int vmsNumber = 5;
			
			vmlist = new ArrayList<Vm>();
			vmlist = WsoExperiment.createVM(brokerId, vmsNumber, 5, 10, 256, 100, 100);

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			// Fifth step: Create Cloudlets
			UtilizationModel utilizationModel = new UtilizationModelStochastic(100);
//	        UtilizationModel utilizationModel = new UtilizationModelFull();
			
			
			int cloudletsNumber = 100;
			cloudletList = new ArrayList<Cloudlet>();
			cloudletList = WsoExperiment.createCloudlet(brokerId, cloudletsNumber, 10000, 10, 100000, 1000, utilizationModel);

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();
			
			printVmList(vmlist, CloudSim.clock());
			
			CloudSim.stopSimulation();

			
			/*
			 * TUTAJ TRZEBA PODMIENIĆ ŻEBY NAM PRINTOWAŁO TO CO CHCEMY!
			 * CAŁKOWITY CZAS
			 * WYDAJNOŚĆ
			 * ZUŻYCIE ENERGII
			 */

			//Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			
//			double utilization = utilizationModel.getUtilization(CloudSim.clock());
//			Log.printLine(utilization);
			printCloudletList(newList);
			
			printExperimentSummary(newList);
			
			Log.printLine();
			Log.printLine("Experiment finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating

		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 2048; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 10000;

		hostList.add(
			new Host(
				hostId,
				new RamProvisionerSimple(ram),
				new BwProvisionerSimple(bw),
				storage,
				peList,
				new VmSchedulerTimeShared(peList)
			)
		); // This is our machine

		// 5. Create a DatacenterCharacteristics object that stores the
		// properties of a data center: architecture, OS, list of
		// Machines, allocation policy: time- or space-shared, time zone
		// and its price (G$/Pe time unit).
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this
										// resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
													// devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	// We strongly encourage users to develop their own broker policies, to
	// submit vms and cloudlets according
	// to the specific rules of the simulated scenario
	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	private static DatacenterBrokerWso createBroker() {
		DatacenterBrokerWso broker = null;
		try {
			/* Add name of allocation algorithm
			 * BF for Best Fit
			 * BFD for Best Fit Decresing
			 * PCBFD for  Power and Computation Capacity Best First Decreasing
			 * if any other string - Best Fit as default
			*/
			broker = new DatacenterBrokerWso("Broker", "BFD");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	/**
	 * Prints the Cloudlet objects.
	 *
	 * @param list list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
				+ "Data center ID" + indent + "VM ID" + indent + "Time" + indent
				+ "Start Time" + indent + "Finish Time" + indent + "Ram Utilization %");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + indent + cloudlet.getResourceId()
						+ indent + indent + indent + cloudlet.getVmId()
						+ indent + indent
						+ dft.format(cloudlet.getActualCPUTime()) + indent
						+ indent + dft.format(cloudlet.getExecStartTime())
						+ indent + indent
						+ dft.format(cloudlet.getFinishTime())
						+ indent + indent
						+ dft.format(cloudlet.getUtilizationOfRam(cloudlet.getActualCPUTime())));
			}
		}
	}
	
	private static void printVmList(List<Vm> list, double time) {
		int size = list.size();
		Vm vm;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Id" + indent + "Vm ID" + indent + "Total CPU Utilization" + indent +
					"Total Utilization Of Cpu Mips");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			vm = list.get(i);
			Log.print((i+1) + indent +  vm.getId() + indent + indent);
			Log.printLine(
					indent +  vm.getTotalUtilizationOfCpu(time)
					+ indent + indent + indent + indent + indent + indent + indent
					+ dft.format(vm.getTotalUtilizationOfCpuMips(time)));
		}
	}
	
	private static void printExperimentSummary(List<Cloudlet> list) {
		double timeArray[];
		double powerArray[];
		double taskSize[];
		Cloudlet cloudlet;
		
		// Every cell is zero by default
		timeArray = new double[5];
		powerArray = new double[5];
		taskSize = new double[5];
		
		Log.printLine();
		Log.printLine("========== SUMMARY ==========");
		
		for (int i = 0; i < list.size(); i++) {
			cloudlet = list.get(i);
			if(timeArray[cloudlet.getVmId()] < cloudlet.getActualCPUTime())
			{
				timeArray[cloudlet.getVmId()] = cloudlet.getActualCPUTime();
			}
		}
		
		double powerUsed = 0;
		for(int i=0; i< list.size(); i++)
		{
			cloudlet = list.get(i);
			powerUsed = powerUsed + cloudlet.getUtilizationOfCpu(cloudlet.getActualCPUTime());
			powerArray[cloudlet.getVmId()] = powerArray[cloudlet.getVmId()] +  cloudlet.getUtilizationOfCpu(cloudlet.getActualCPUTime());
			taskSize[cloudlet.getVmId()] = taskSize[cloudlet.getVmId()] + cloudlet.getCloudletTotalLength();
		}
		
		DecimalFormat dftSummary = new DecimalFormat("###########.##");
		double worstTime = 0;
		int worstTimeVMId = 0;
		
		double worstPowerUsage = 0;
		int worstpowerVMId = 0;
		for(int i=0;i < 5; ++i){
			if(timeArray[i] > worstTime)
			{
				worstTime = timeArray[i];
				worstTimeVMId = i;
			}
			
			if(powerArray[i] > worstPowerUsage)
			{
				worstPowerUsage = powerArray[i];
				worstpowerVMId = i;
			}
			Log.printLine("VM id: " + i +  ", VM time: " + dftSummary.format(timeArray[i]/1000) +
					"[s], power used: " + dftSummary.format(powerArray[i]) + "[W], total task size: " + 
					dftSummary.format(taskSize[i]) + ", efficiency: " + taskSize[i]/powerArray[i]);
		}
		
		Log.printLine("Worst time (total time): " + dftSummary.format(worstTime) + "[s] on VM: "+
		worstTimeVMId);
		
		// TODO: Watts?
		Log.printLine("Total power used: " + dftSummary.format(powerUsed) + "[W], worst power usage: " 
		+ dftSummary.format(worstPowerUsage) + "[W] on VM:" + worstpowerVMId);
	}
	
  public static List<Vm> createIdenticalVM(int brokerId, int vms) {
        //Creates list of VM
        List<Vm> list = new ArrayList<Vm>();

        //VM Parameters
		int mips = 10;
		long size = 10000; // image size (MB)
		int ram = 256; // vm memory (MB)
		long bw = 1000;
		int pesNumber = 1; // number of cpus
		String vmm = "Xen"; // VMM name

        for (int i = 0; i < vms; i++) {
        	int vmid = i;
        	Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            list.add(vm);
        }

        return list;
    }
  
  public static List<Vm> createVM(int brokerId, int vms, int maxMips, long maxSize, int maxRam, long maxBw, int maxPesNumber) {
        //Creates list of VM
        List<Vm> list = new ArrayList<Vm>();

        //VM Parameters
		int minMips = 1;
		long minSize = 1; // image size (MB)
		int minRam = 1; // vm memory (MB)
		long minBw = 1;
		int minPesNumber = 1; // number of cpus
		String vmm = "Xen"; // VMM name

        for (int i = 0; i < vms; i++) {
        	int vmid = i;
        	//VM Parameters
        	int mips = getRandomNumber(minMips, maxMips);
        	long size = getRandomNumber(minSize, maxSize);
        	int ram = getRandomNumber(minRam, maxRam);
        	long bw = getRandomNumber(minBw, maxBw);
        	int pesNumber = getRandomNumber(minPesNumber, maxPesNumber);
        	
        	Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            list.add(vm);
        }

        return list;
    }

    public static List<Cloudlet> createIdenticalCloudlet(int userId, int cloudlets) {
        // Creates a container to store Cloudlets
        LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

        //cloudlet parameters
        long length = 1000000000;
        long fileSize = 300000000;
        long outputSize = 300000000;
        int pesNumber = 2;
        //UtilizationModel utilizationModel = new UtilizationModelStochastic(100);
        UtilizationModel utilizationModel = new UtilizationModelFull();

        
        for (int i = 0; i < cloudlets; i++) {
        	int id = i;
            //length+=100;
        	Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
            // setting the owner of these Cloudlets
            cloudlet.setUserId(userId);
            list.add(cloudlet);
        }

        return list;
    }
    
    public static List<Cloudlet> createCloudlet(int userId, int cloudlets, int maxLength, long maxFileSize, long maxOutputSize, int maxPesNumber, UtilizationModel utilizationModel ) {
        // Creates a container to store Cloudlets
        LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

        //cloudlet parameters
        long minLength = 10;
        long minFileSize = 10;
        long minOutputSize = 10;
        int minPesNumber = 1;
                
        for (int i = 0; i < cloudlets; i++) {
        	int id = i;
        	//cloudlet parameters
        	long length = getRandomNumber(minLength, maxLength);
	        long fileSize = getRandomNumber(minFileSize, maxFileSize);
	        long outputSize = getRandomNumber(minOutputSize, maxOutputSize);
	        int pesNumber = getRandomNumber(minPesNumber, maxPesNumber);
            
        	Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
            // setting the owner of these Cloudlets
            cloudlet.setUserId(userId);
            list.add(cloudlet);
        }

        return list;
    }
    
    private static int getRandomNumber(int min, int max) {
    	WsoExperiment.seed = WsoExperiment.seed + 1;
    	Random generator = new Random(seed);
        return (int) ((generator.nextDouble() * (max - min)) + min);
    }
    
    private static long getRandomNumber(long min, long max) {
    	WsoExperiment.seed = WsoExperiment.seed + 1;
    	Random generator = new Random(seed);
        return (long) ((generator.nextDouble() * (max - min)) + min);
    }

}
