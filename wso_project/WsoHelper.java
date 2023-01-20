package wso_project;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelNull;
import org.cloudbus.cloudsim.UtilizationModelStochastic;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.power.PowerDatacenterBroker;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.power.PowerHostUtilizationHistory;
import org.cloudbus.cloudsim.power.PowerVm;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


public class WsoHelper {
	public static int seed = WsoConstants.CLOUDLET_SEED;
	/**
	 * Creates the broker.
	 * 
	 * @return the datacenter broker
	 */
	public static DatacenterBroker createBroker(String allocationAlgorithm, int numberOfVMs) {
		
		DatacenterBroker broker = null;
		try {		
			if (allocationAlgorithm == null) {
				broker = new PowerDatacenterBroker("Broker");
			}
			else {
				
				broker = new DatacenterBrokerWso("Broker", allocationAlgorithm, numberOfVMs);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return broker;
	}
	/**
	 * Creates the cloudlet list.
	 * 
	 * @param brokerId the broker id
	 * @param cloudletsNumber the cloudlets number
	 * 
	 * @return the list< cloudlet>
	 */
	public static List<Cloudlet> createCloudletList(int brokerId, int cloudletsNumber, String allocationAlgorithm) {
		setSeed(WsoConstants.CLOUDLET_SEED);
		List<Cloudlet> list = new ArrayList<Cloudlet>();
		long seed = WsoConstants.CLOUDLET_UTILIZATION_SEED;
		UtilizationModel utilizationModelNull = new UtilizationModelNull();

		for (int i = 0; i < cloudletsNumber; i++) {
			Cloudlet cloudlet = null;
			
	        //cloudlet parameters
	        long length = getRandomNumber(WsoConstants.CLOUDLET_MIN_LENGTH, WsoConstants.CLOUDLET_MAX_LENGTH);
		    long fileSize = getRandomNumber(WsoConstants.CLOUDLET_MIN_FILESIZE, WsoConstants.CLOUDLET_MAX_FILESIZE);
		    long outputSize = getRandomNumber(WsoConstants.CLOUDLET_MIN_OUTPUT_SIZE, WsoConstants.CLOUDLET_MAX_OUTPUT_SIZE);
		    int pesNumber = getRandomNumber(WsoConstants.CLOUDLET_MIN_PES, WsoConstants.CLOUDLET_MAX_PES);

			if (seed == -1) {
				cloudlet = new Cloudlet(
						i,
						length,
						pesNumber,
						fileSize,
						outputSize,
						new UtilizationModelStochastic(),
						utilizationModelNull,
						utilizationModelNull);
			} else {
				cloudlet = new Cloudlet(
						i,
						length,
						pesNumber,
						fileSize,
						outputSize,
						new UtilizationModelStochastic(seed * i),
						utilizationModelNull,
						utilizationModelNull);
			}
			cloudlet.setUserId(brokerId);
			if (allocationAlgorithm == null) {
				cloudlet.setVmId(i % WsoConstants.NUMBER_OF_VMS);
			}
			list.add(cloudlet);
		}
		
		return list;
	}
	
	public static List<Vm> createVmList(int brokerId, int vmsNumber) {
		List<Vm> vms = new ArrayList<Vm>();
		for (int i = 0; i < vmsNumber; i++) {
			int vmType = i / (int) Math.ceil((double) vmsNumber / WsoConstants.VM_TYPES);
			vms.add(new PowerVm(
					i,
					brokerId,
					WsoConstants.VM_MIPS[vmType],
					WsoConstants.VM_PES[vmType],
					WsoConstants.VM_RAM[vmType],
					WsoConstants.VM_BW,
					WsoConstants.VM_SIZE,
					1,
					"Xen",
					//CloudletSchedulerDynamicWorkload(WsoConstants.VM_MIPS[vmType], WsoConstants.VM_PES[vmType]),
					new CloudletSchedulerDynamicWorkload(WsoConstants.VM_MIPS[vmType], WsoConstants.VM_PES[vmType]),
					WsoConstants.SCHEDULING_INTERVAL));
		}
		return vms;
	}
	
	/**
	 * Creates the host list.
	 * 
	 * @param hostsNumber the hosts number
	 * 
	 * @return the list< power host>
	 */
	public static List<PowerHost> createHostList(int hostsNumber) {
		List<PowerHost> hostList = new ArrayList<PowerHost>();
		for (int i = 0; i < hostsNumber; i++) {
			int hostType = i % WsoConstants.HOST_TYPES;

			List<Pe> peList = new ArrayList<Pe>();
			for (int j = 0; j < WsoConstants.HOST_PES[hostType]; j++) {
				peList.add(new Pe(j, new PeProvisionerSimple(WsoConstants.HOST_MIPS[hostType])));
			}

			hostList.add(new PowerHostUtilizationHistory(
					i,
					new RamProvisionerSimple(WsoConstants.HOST_RAM[hostType]),
					new BwProvisionerSimple(WsoConstants.HOST_BW),
					WsoConstants.HOST_STORAGE,
					peList,
					new VmSchedulerTimeSharedOverSubscription(peList),
					WsoConstants.HOST_POWER[hostType]));
		}
		return hostList;
	}
	
	/**
	 * Creates the datacenter.
	 * 
	 * @param name the name
	 * @param datacenterClass the datacenter class
	 * @param hostList the host list
	 * @param vmAllocationPolicy the vm allocation policy
	 * @param simulationLength
	 * 
	 * @return the power datacenter
	 * 
	 * @throws Exception the exception
	 */
	public static Datacenter createDatacenter(
			String name,
			Class<? extends Datacenter> datacenterClass,
			List<PowerHost> hostList,
			VmAllocationPolicy vmAllocationPolicy) throws Exception {
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this resource
		double costPerBw = 0.0; // the cost of using bw in this resource

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch,
				os,
				vmm,
				hostList,
				time_zone,
				cost,
				costPerMem,
				costPerStorage,
				costPerBw);

		Datacenter datacenter = null;
		try {
			datacenter = datacenterClass.getConstructor(
					String.class,
					DatacenterCharacteristics.class,
					VmAllocationPolicy.class,
					List.class,
					Double.TYPE).newInstance(
					name,
					characteristics,
					vmAllocationPolicy,
					new LinkedList<Storage>(),
					WsoConstants.SCHEDULING_INTERVAL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		return datacenter;
	}
	
	private static void setSeed(int cloudletSeed) {
		seed = cloudletSeed;
		
	}
	
	private static int getRandomNumber(int min, int max) {
    	seed = seed + 100;
    	Random generator = new Random(seed);
        return (int) ((generator.nextDouble() * (max - min)) + min);
    }
    
    private static long getRandomNumber(long min, long max) {
    	seed = seed + 100;
    	Random generator = new Random(seed);
        return (long) ((generator.nextDouble() * (max - min)) + min);
    }
}