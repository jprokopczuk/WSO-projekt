package wso_project;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class DatacenterBrokerWso extends DatacenterBroker {
	
	private String allocationAlgorithm;
	private ArrayList<Long> vmCurrRam;
	
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
				vm = getBestFitVm(cloudlet);
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
		
		vmCurrRam.clear();
		
	}
	
	protected Vm getBestFitVm(Cloudlet cloudlet) {
		List <Vm> vmList = getVmsCreatedList();
		ArrayList <Long> ramDiff = new ArrayList<Long>();
		/*
		 * TUTAJ TRZEBA PODMIENIĆ TO GET CLOUDLET TOTAL NA ODPOWIEDNIĄ RZECZ!
		 * W ZALEŻNOŚCI OD TEGO JAKA TO BĘDZIE JEDNOSTKA CZY BAJTY CZY MB ZAMIANIC MNOZNIK W 113 LINII
		 * 
		 * TRZEBA DOPISAC ALGORYTM Power and Computation Capacity Best First Decreasing
		 * JAKIES ZRODLO O CO W TYM CHODZI???
		 */
		long cloudletSize = cloudlet.getCloudletTotalLength();
		
		for (int i=0; i < vmList.size(); i++) {				
			long vmRamByte = (vmList.get(i).getCurrentAllocatedRam() * 1000000) - vmCurrRam.get(i);
			long diff = vmRamByte - cloudletSize;
			
			if (diff < 0) {
				diff = cloudletSize * 10000000;
			}
			ramDiff.add(diff);
		}
		int indexOfMinimum = ramDiff.indexOf(Collections.min(ramDiff));
		long newRamValue = vmCurrRam.get(indexOfMinimum) + cloudletSize;
		vmCurrRam.set(indexOfMinimum, newRamValue);
		
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
	
	private void setVmCurrAllocationList() {
		this.vmCurrRam = new ArrayList<Long>();
		long zero = 0;
		for (int i=0; i< getVmsCreatedList().size(); i++) {
			vmCurrRam.add(zero);
		}
	}

}
