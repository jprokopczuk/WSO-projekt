package wso_project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.power.PowerHost;

public class DatacenterBrokerWso extends DatacenterBroker {
	
	private String allocationAlgorithm;
	private ArrayList<Long> vmCurrRAM;
	private static int numOfVMs;
	
	public DatacenterBrokerWso(String name, String allocationAlgorithm, int numberOfVMs) throws Exception {
		super(name);
		numOfVMs = numberOfVMs;

		setVmList(new ArrayList<Vm>());
		setVmsCreatedList(new ArrayList<Vm>());
		setCloudletList(new ArrayList<Cloudlet>());
		setCloudletSubmittedList(new ArrayList<Cloudlet>());
		setCloudletReceivedList(new ArrayList<Cloudlet>());

		cloudletsSubmitted = 0;
		setVmsRequested(0);
		setVmsAcks(0);
		setVmsDestroyed(0);

		setDatacenterIdsList(new LinkedList<Integer>());
		setDatacenterRequestedIdsList(new ArrayList<Integer>());
		setVmsToDatacentersMap(new HashMap<Integer, Integer>());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());
		setCloudletAllocationAlgorithm(allocationAlgorithm);
		setVmCurrAllocationList(numberOfVMs);
	}
	
	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		
//		long newRAM= vmCurrRAM.get(cloudlet.getVmId()) - cloudlet.getCloudletTotalLength();
//		vmCurrRAM.set(cloudlet.getVmId(), newRAM);
		
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId()
				+ " received");
		cloudletsSubmitted--;
		if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else { // some cloudlets haven't finished yet
			if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
				// all the cloudlets sent finished. It means that some bount
				// cloudlet is waiting its VM be created
				clearDatacenters();
				createVmsInDatacenter(0);
			}

		}
	}


	
	@Override
	protected void submitCloudlets() {
		List <Cloudlet> cloudletSubmitList = getCloudletList();
		
		if (allocationAlgorithm == "BFD") {
			cloudletSubmitList = sortCloudletList(cloudletSubmitList);
		}
			
		for (Cloudlet cloudlet : cloudletSubmitList) {
			Vm vm;
			// if user didn't bind this cloudlet and it has not been executed yet
			if (cloudlet.getVmId() == -1) {
				if (allocationAlgorithm == "PCA-BFD") {
					vm = getPCABestFitVm(cloudlet);
				}
				else {
					vm = getBestFitVm(cloudlet);
				}
				if(vm == null) {
					setVmCurrAllocationList(numOfVMs);
					if (allocationAlgorithm == "PCA-BFD") {
						vm = getPCABestFitVm(cloudlet);
					}
					else {
						vm = getBestFitVm(cloudlet);
					}
				}
					
			} else { // submit to the specific vm
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId() + ": bount VM not available");
					continue;
				}
			}

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet "
					+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			getCloudletSubmittedList().add(cloudlet);
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}
	
	protected Vm getBestFitVm(Cloudlet cloudlet) {
		List <Vm> vmList = getVmsCreatedList();
		ArrayList <Long> ramDiff = new ArrayList<Long>();
		int hugeNumber = 999999999;
		long cloudletNumRAM = cloudlet.getCloudletTotalLength();
		
		for (int i=0; i < vmList.size(); i++) {				
			long vmNumRam = (vmList.get(i).getRam() - vmCurrRAM.get(i)) * 1000000;
			long diff = vmNumRam - cloudletNumRAM;
			
			if (diff < 0) {
				diff = hugeNumber;
			}
			ramDiff.add(diff);
		}
		
		// if there is no VM with enough memory
		int numVM = 0;
		for (long element : ramDiff) {
		  if (element == hugeNumber) numVM++;
		}
		
		if (numVM == ramDiff.size()){
			return null;
		}
		
		int indexOfMinimum = ramDiff.indexOf(Collections.min(ramDiff));
		long newRamValue = vmCurrRAM.get(indexOfMinimum) + cloudletNumRAM;
		vmCurrRAM.set(indexOfMinimum, newRamValue);
		return vmList.get(indexOfMinimum);
	}
	
	protected Vm getPCABestFitVm(Cloudlet cloudlet) {
		List <Vm> vmList = getVmsCreatedList();
		ArrayList <Double> ramRatioDiff = new ArrayList<Double>();
		int hugeNumber = 999999999;
		long cloudletNumRAM = cloudlet.getCloudletTotalLength();
		
		for (int i=0; i < vmList.size(); i++) {				
			long vmNumRam = (vmList.get(i).getRam() - vmCurrRAM.get(i)) * 1000000;
			PowerHost host = (PowerHost) vmList.get(i).getHost();
			
			double ratio = host.getMaxPower() / vmNumRam;
			
			if (vmNumRam == 0) {
				ratio = hugeNumber;
			}
			
			ramRatioDiff.add(ratio);
		}
		
		// if there is no VM with enough memory
		int numVM = 0;
		for (double element : ramRatioDiff) {
		  if (element == hugeNumber) numVM++;
		}
		
		if (numVM == ramRatioDiff.size()){
			return null;
		}
		
		int indexOfMinimum = ramRatioDiff.indexOf(Collections.min(ramRatioDiff));
		long newRamValue = vmCurrRAM.get(indexOfMinimum) + cloudletNumRAM;
		vmCurrRAM.set(indexOfMinimum, newRamValue);
		return vmList.get(indexOfMinimum);
	}
	
	private List<Cloudlet> sortCloudletList(List<Cloudlet> cloudletSubmitList) {
		List<Cloudlet> sortedCloudletList = cloudletSubmitList.stream()
		        .sorted(Comparator.comparingLong(Cloudlet::getCloudletTotalLength).reversed())
		        .collect(Collectors.toList());
		return sortedCloudletList;
	}
	
	private void setCloudletAllocationAlgorithm(String allocationAlgorithm) {
		this.allocationAlgorithm = allocationAlgorithm;
	}
	
	private void setVmCurrAllocationList(int numberOfVms) {
		vmCurrRAM = new ArrayList <Long> ();
		long zero = 0;
		for (int i = 0; i< numberOfVms; i++) {
			vmCurrRAM.add(zero);
		}
	}
}