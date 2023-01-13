package wso_project;

import java.util.List;
import java.io.*;
import java.util.*;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;


public class CloudletAllocation {
	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<Vm> vmlist;
	
	public CloudletAllocation(List<Cloudlet> cloudletList, List<Vm> vmlist) {
		this.cloudletList = cloudletList;
		this.vmlist = vmlist;
	 }
	
	public static List<Cloudlet> assignCloudletToVm(){
		for (int i = 0; i < cloudletList.size(); i++) {
			int vmId = vmlist.get(i).getId();
			cloudletList.get(i).setVmId(vmId);
		}
		return cloudletList;
	}
	
	Comparator<Cloudlet> compareBySize = new Comparator<Cloudlet>() {
		@Override
		public int compare(Cloudlet c1, Cloudlet c2) {
			return c1.getId().compareTo(c2.getId());
		}
	};
	
	Comparator<Vm> compareVmBySize = new Comparator<Vm>() {
		@Override
		public int compare(Vm v1, Vm v2) {
			return v1.getRam().compareTo(v2.getRam());
		}
	};
}
