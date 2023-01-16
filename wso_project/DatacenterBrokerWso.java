package wso_project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.power.PowerDatacenterBroker;

public class DatacenterBrokerWso extends PowerDatacenterBroker {
	
	private String allocationAlgorithm;
	private ArrayList<Integer> vmCurrCPU;
	
	public DatacenterBrokerWso(String name, String allocationAlgorithm) throws Exception {
		super(name);

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
		setVmCurrAllocationList();
	}
	
	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
			case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
				processResourceCharacteristicsRequest(ev);
				break;
			// Resource characteristics answer
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
			// A finished cloudlet returned
			case CloudSimTags.CLOUDLET_RETURN:
				processCloudletReturn(ev);
				submitCloudlets();
				break;
			// if the simulation finishes
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}

	
	@Override
	protected void submitCloudlets() {
		List <Cloudlet> cloudletSubmitList = getCloudletList();
		
		setVmCurrAllocationList();
		if (allocationAlgorithm == "BFD") {
			cloudletSubmitList = sortCloudletList(cloudletSubmitList);
		}
			
		for (Cloudlet cloudlet : cloudletSubmitList) {
			Vm vm;
			// if user didn't bind this cloudlet and it has not been executed yet
			if (cloudlet.getVmId() == -1) {
				if (allocationAlgorithm == "EPOBF") {
					vm = getEPOBFVm(cloudlet);
				}
				else {
					vm = getBestFitVm(cloudlet);
				}
				if (vm == null) {
					continue;
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
//			vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
			getCloudletSubmittedList().add(cloudlet);
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
		
		vmCurrCPU.clear();
	}
	
	protected Vm getBestFitVm(Cloudlet cloudlet) {
		List <Vm> vmList = getVmsCreatedList();
		ArrayList <Integer> cpuDiff = new ArrayList<Integer>();
		/*
		 * TUTAJ TRZEBA PODMIENIĆ TO GET CLOUDLET TOTAL NA ODPOWIEDNIĄ RZECZ!
		 * W ZALEŻNOŚCI OD TEGO JAKA TO BĘDZIE JEDNOSTKA CZY BAJTY CZY MB ZAMIANIC MNOZNIK W 113 LINII
		 * 
		 * TRZEBA DOPISAC ALGORYTM Power and Computation Capacity Best First Decreasing
		 * JAKIES ZRODLO O CO W TYM CHODZI???
		 */
		int cloudletNumCPU = cloudlet.getNumberOfPes();
		
		for (int i=0; i < vmList.size(); i++) {				
			int vmNumCPU = vmList.get(i).getNumberOfPes() - vmCurrCPU.get(i);
			int diff = vmNumCPU - cloudletNumCPU;
			
			if (diff < 0) {
				diff = 1000;
			}
			cpuDiff.add(diff);
		}
		
		int indexOfMinimum = cpuDiff.indexOf(Collections.min(cpuDiff));
		int newCPUValue = vmCurrCPU.get(indexOfMinimum) + cloudletNumCPU;
		
		// if there is no VM with enough number of CPU
		int numVM = 0;
		for (int element : cpuDiff) {
		  if (element == 1000) numVM++;
		}
		
		if (numVM == cpuDiff.size()){
			return null;
		}
		vmCurrCPU.set(indexOfMinimum, newCPUValue);
		return vmList.get(indexOfMinimum);
	}
	
	protected Vm getEPOBFVm(Cloudlet cloudlet) {
		List <Vm> vmList = getVmsCreatedList();
		ArrayList <Integer> cpuDiff = new ArrayList<Integer>();
		List<Double> energyRatioList = new ArrayList<Double>();
		double cloudletCost = cloudlet.getProcessingCost();

		int cloudletNumCPU = cloudlet.getNumberOfPes();
		
		
		for (int i=0; i < vmList.size(); i++) {				
			int vmNumCPU = vmList.get(i).getNumberOfPes() - vmCurrCPU.get(i);
//			Host host = vmList.get(i).getHost().get;
			int diff = vmNumCPU;
			
			if (diff < 0) {
				diff = 0;
			}
			energyRatioList.add(diff / cloudletCost);
		}
		int indexOfMaximum = energyRatioList.indexOf(Collections.max(energyRatioList));
		int newCPUValue = vmCurrCPU.get(indexOfMaximum) + cloudletNumCPU;
		
		// if there is no VM with enough number of CPU
		int numVM = 0;
		for (double element : energyRatioList) {
		  if (element == 0.0) numVM++;
		}
		
		if (numVM == energyRatioList.size()){
			return null;
		}
		vmCurrCPU.set(indexOfMaximum, newCPUValue);
		return vmList.get(indexOfMaximum);
	}
	
//	protected Vm getEPOBFVm(Cloudlet cloudlet) {
//		List <Vm> vmList = getVmsCreatedList();
//		double cloudletCost = cloudlet.getProcessingCost();
//		List<Double> energyRatioList = new ArrayList<Double>();
//		
//		for (int i=0; i < vmList.size(); i++) {				
//			int vmNumCPU = vmList.get(i).getNumberOfPes();
//			double ratio = cloudletCost / vmNumCPU;
//			energyRatioList.add(ratio);
//		}
//		
//		int indexOfMaximum = energyRatioList.indexOf(Collections.min(energyRatioList));
//		return vmList.get(indexOfMaximum);
//	}
	
	private List<Cloudlet> sortCloudletList(List<Cloudlet> cloudletSubmitList) {

		List<Cloudlet> sortedCloudletList = cloudletSubmitList.stream()
		        .sorted(Comparator.comparingLong(Cloudlet::getCloudletTotalLength).reversed())
		        .collect(Collectors.toList());
		return sortedCloudletList;
	}
	
	private void setCloudletAllocationAlgorithm(String allocationAlgorithm) {
		this.allocationAlgorithm = allocationAlgorithm;
	}
	
	private void setVmCurrAllocationList() {
		this.vmCurrCPU = new ArrayList<Integer>();
		Log.printLine(getVmsCreatedList().size());
		for (int i=0; i< getVmsCreatedList().size(); i++) {
			vmCurrCPU.add(0);
		}
	}

}
