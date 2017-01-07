package cn.base.dfs.status;

import java.util.HashMap;
import java.util.Map;

public class NodeStatus {

	// namenode start status
	private boolean nameNodeisRun = false;
	// datanode start status
	private boolean dataNodeisRun = false;

	public boolean isNameNodeisRun() {
		return nameNodeisRun;
	}

	public void setNameNodeisRun(boolean nameNodeisRun) {
		this.nameNodeisRun = nameNodeisRun;
	}

	public boolean isDataNodeisRun() {
		return dataNodeisRun;
	}

	public void setDataNodeisRun(boolean dataNodeisRun) {
		this.dataNodeisRun = dataNodeisRun;
	}

}
