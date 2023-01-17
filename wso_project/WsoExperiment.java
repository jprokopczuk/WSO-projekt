package wso_project;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.examples.power.Constants;
import org.cloudbus.cloudsim.examples.power.Helper;
import org.cloudbus.cloudsim.power.PowerDatacenterNonPowerAware;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.power.PowerVmAllocationPolicySimple;

public class WsoExperiment {


	/**
	 * Creates main() to run this example.
	 * 
	 * @param args the args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String experimentName = "wso_experiment";
		String outputFolder = "output";

		Log.setDisabled(!Constants.ENABLE_OUTPUT);
		Log.printLine("Starting " + experimentName);

		try {
			CloudSim.init(1, Calendar.getInstance(), false);

			/*
			 * Choose allocation algorithm
			 * null if none
			 * BF if Best Fit
			 * BFD if Best Fit Decreasing 
			 * PCA-BFD if Power and Computation Capacity Best First Decreasing
			*/
			String allocationAlgorithm = "PCA-BFD";
			DatacenterBroker broker = WsoHelper.createBroker(allocationAlgorithm);
			int brokerId = broker.getId();

			List<Cloudlet> cloudletList = WsoHelper.createCloudletList(
					brokerId,
					WsoConstants.NUMBER_OF_CLOUDLETS,
					allocationAlgorithm);
			List<Vm> vmList = Helper.createVmList(brokerId, WsoConstants.NUMBER_OF_VMS);
			List<PowerHost> hostList = Helper.createHostList(WsoConstants.NUMBER_OF_HOSTS);

			PowerDatacenterNonPowerAware datacenter = (PowerDatacenterNonPowerAware) Helper.createDatacenter(
					"Datacenter",
					PowerDatacenterNonPowerAware.class,
					hostList,
					new PowerVmAllocationPolicySimple(hostList));

			datacenter.setDisableMigrations(true);

			broker.submitVmList(vmList);
			broker.submitCloudletList(cloudletList);

			double lastClock = CloudSim.startSimulation();

			List<Cloudlet> newList = broker.getCloudletReceivedList();
			Log.printLine("Received " + newList.size() + " cloudlets");

			CloudSim.stopSimulation();
			
//			Helper.printCloudletList(cloudletList);

			Helper.printResults(
					datacenter,
					vmList,
					lastClock,
					experimentName,
					WsoConstants.OUTPUT_CSV,
					outputFolder);

		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
			System.exit(0);
		}

		Log.printLine("Finished " + experimentName);
	}
}