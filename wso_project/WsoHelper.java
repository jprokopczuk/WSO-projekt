package wso_project_wersjaRAM;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelNull;
import org.cloudbus.cloudsim.UtilizationModelStochastic;
import org.cloudbus.cloudsim.examples.power.Constants;
import org.cloudbus.cloudsim.examples.power.random.RandomConstants;
import org.cloudbus.cloudsim.power.PowerDatacenterBroker;

import wso_project.WsoExperiment;

public class WsoHelper {
	/**
	 * Creates the broker.
	 * 
	 * @return the datacenter broker
	 */
	public static DatacenterBroker createBroker(String allocationAlgorithm) {
		DatacenterBroker broker = null;
		try {		
			if (allocationAlgorithm == null) {
				broker = new PowerDatacenterBroker("Broker");
			}
			else {
				
				broker = new DatacenterBrokerWso("Broker", allocationAlgorithm);
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
	
	private static int getRandomNumber(int min, int max) {
    	int seed = WsoConstants.CLOUDLET_SEED + 100;
    	Random generator = new Random(seed);
        return (int) ((generator.nextDouble() * (max - min)) + min);
    }
    
    private static long getRandomNumber(long min, long max) {
    	int seed = WsoConstants.CLOUDLET_SEED + 100;
    	Random generator = new Random(seed);
        return (long) ((generator.nextDouble() * (max - min)) + min);
    }
}
