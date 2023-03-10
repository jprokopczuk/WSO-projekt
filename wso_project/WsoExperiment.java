package wso_project;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerDatacenterNonPowerAware;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.power.PowerVmAllocationPolicySimple;

public class WsoExperiment {

	static List<Double> time = new ArrayList<Double>();
	static List<Double> energy = new ArrayList<Double>();
	static List<Long> sizeSum = new ArrayList<Long>();
	
	/**
	 * Creates main() to run this example.
	 * 
	 * @param args the args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String experimentName = "wso_experiment";

		Log.setDisabled(!WsoConstants.ENABLE_OUTPUT);
		Log.printLine("Starting " + experimentName);
		
		int[] cloudletsNumber= {10, 220, 1000, 4000, 60000};
		int[] hostsNumber = {5, 5, 5, 5, 5};
		int[] vmsPerHostNumber = {30, 30, 30, 30, 30};

		try {
			/*
			 * Choose allocation algorithm
			 * null if none
			 * BF if Best Fit
			 * BFD if Best Fit Decreasi
			 * ng 
			 * PCA-BFD if Power and Computation Capacity Best First Decreasing
			*/
			String allocationAlgorithm = "BF";
			for(int i = 0; i < cloudletsNumber.length; i++) {
				makeExperimentWithParameters(cloudletsNumber[i], vmsPerHostNumber[i], 
						hostsNumber[i], allocationAlgorithm);
			}
			printSummary();

		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
			System.exit(0);
		}

		Log.printLine("Finished " + experimentName);
	}
	
	public static void makeExperimentWithParameters(int numberOfCloudLets, int numberOfVMs, 
			int numebrOfHosts, String allocationAlgorithm) throws Exception {
		CloudSim.init(1, Calendar.getInstance(), false);
//		String experimentName = "wso_experiment";
//		String outputFolder = "output";

		DatacenterBroker broker = WsoHelper.createBroker(allocationAlgorithm, numberOfVMs);
		int brokerId = broker.getId();

		List<Cloudlet> cloudletList = WsoHelper.createCloudletList(
				brokerId,
				numberOfCloudLets,
				allocationAlgorithm);
		List<Vm> vmList = WsoHelper.createVmList(brokerId, numberOfVMs);
		List<PowerHost> hostList = WsoHelper.createHostList(numebrOfHosts);

		PowerDatacenterNonPowerAware datacenter = (PowerDatacenterNonPowerAware) WsoHelper.createDatacenter(
				"Datacenter",
				PowerDatacenterNonPowerAware.class,
				hostList,
				new PowerVmAllocationPolicySimple(hostList));

		datacenter.setDisableMigrations(true);

		broker.submitVmList(vmList);
		broker.submitCloudletList(cloudletList);

		double lastClock = CloudSim.startSimulation();

		CloudSim.stopSimulation();
		
		List<Cloudlet> newList = broker.getCloudletReceivedList();
		Log.printLine("Received " + newList.size() + " cloudlets");
		
		time.add(lastClock);
		energy.add(datacenter.getPower() / (3600 * 1000));
		
		long tempSizeSum = 0;
		for(Cloudlet i : cloudletList) {
			tempSizeSum = tempSizeSum + i.getCloudletLength();
		}
		sizeSum.add(tempSizeSum);
			
//		Helper.printResults(
//				datacenter,
//				vmList,
//				lastClock,
//				experimentName,
//				WsoConstants.OUTPUT_CSV,
//				outputFolder);
	}
	
	
	public static void printSummary() {
		Log.printLine("-------- SUMMARY --------");
		for(int i=0; i< time.size(); i++) {
			Log.printLine("Energy: " + energy.get(i) + ", Time: " + time.get(i) + ", Effi.: " + sizeSum.get(i)/energy.get(i));
		}
	}
}