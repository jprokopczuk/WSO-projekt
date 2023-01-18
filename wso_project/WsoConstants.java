package wso_project;

import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G4Xeon3040;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G5Xeon3075;

public class WsoConstants {
	public final static int NUMBER_OF_VMS = 10;

	public final static int NUMBER_OF_HOSTS = 10;
	
	public final static int NUMBER_OF_CLOUDLETS = 1;
	
	public final static long CLOUDLET_UTILIZATION_SEED = 100;
	public final static int CLOUDLET_SEED = 100;
	
	public final static boolean ENABLE_OUTPUT = true;
	public final static boolean OUTPUT_CSV    = false;

	public final static double SCHEDULING_INTERVAL = 300;
	public final static double SIMULATION_LIMIT = 24 * 60 * 60;

	public final static long CLOUDLET_MIN_LENGTH	= 10;
	public final static long CLOUDLET_MAX_LENGTH	= 15 * (int) SIMULATION_LIMIT;
	
	public final static int CLOUDLET_MIN_PES	= 1;
	public final static int CLOUDLET_MAX_PES	= 10;
	
	public final static long CLOUDLET_MIN_FILESIZE	= 1;
	public final static long CLOUDLET_MAX_FILESIZE	= 100;
	
	public final static long CLOUDLET_MIN_OUTPUT_SIZE	= 1;
	public final static long CLOUDLET_MAX_OUTPUT_SIZE	= 100;

	/*
	 * VM instance types:
	 *   High-Memory Extra Large Instance: 3.25 EC2 Compute Units, 8.55 GB // too much MIPS
	 *   High-CPU Medium Instance: 2.5 EC2 Compute Units, 0.85 GB
	 *   Extra Large Instance: 2 EC2 Compute Units, 3.75 GB
	 *   Small Instance: 1 EC2 Compute Unit, 1.7 GB
	 *   Micro Instance: 0.5 EC2 Compute Unit, 0.633 GB
	 *   We decrease the memory size two times to enable oversubscription
	 *
	 */
	public final static int VM_TYPES	= 4;
	public final static int[] VM_MIPS	= { 2500, 2000, 1000, 500 };
	public final static int[] VM_PES	= { 1, 1, 1, 1 };
	public final static int[] VM_RAM	= { 512,  2048, 256, 1024 };
	public final static int VM_BW		= 100000; // 100 Mbit/s
	public final static int VM_SIZE		= 2500; // 2.5 GB

	/*
	 * Host types:
	 *   HP ProLiant ML110 G4 (1 x [Xeon 3040 1860 MHz, 2 cores], 4GB)
	 *   HP ProLiant ML110 G5 (1 x [Xeon 3075 2660 MHz, 2 cores], 4GB)
	 *   We increase the memory size to enable over-subscription (x4)
	 */
	public final static int HOST_TYPES	 = 2;
	public final static int[] HOST_MIPS	 = { 1860, 2660 };
	public final static int[] HOST_PES	 = { 2, 2 };
	public final static int[] HOST_RAM	 = { 4096, 4096 };
	public final static int HOST_BW		 = 1000000; // 1 Gbit/s
	public final static int HOST_STORAGE = 1000000; // 1 GB

	public final static PowerModel[] HOST_POWER = {
		new PowerModelSpecPowerHpProLiantMl110G4Xeon3040(),
		new PowerModelSpecPowerHpProLiantMl110G5Xeon3075()
	};

}