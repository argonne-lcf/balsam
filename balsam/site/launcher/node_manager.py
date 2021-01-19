class InsufficientResources(Exception):
    pass


class NodeManager:
    def __init__(self, node_list, allow_node_packing=True):
        self.nodes = node_list
        self.job_node_map = {}
        self.allow_node_packing = allow_node_packing

    def _assign_single_node(self, job_id, num_cpus, num_gpus, node_occupancy):
        if not self.allow_node_packing:
            node_occupancy = 1.0
        for node_idx, node in enumerate(self.nodes):
            if node.check_fit(num_cpus, num_gpus, node_occupancy):
                spec = node.assign(job_id, num_cpus, num_gpus, node_occupancy)
                spec["node_id"] = node.node_id
                spec["hostname"] = node.node_id
                self.job_node_map[job_id] = [node_idx]
                return [spec]
        raise InsufficientResources

    def _assign_multi_node(self, job_id, num_nodes):
        assigned_nodes = []
        assigned_idxs = []
        for node_idx, node in enumerate(self.nodes):
            if node.check_fit(num_cpus=0, num_gpus=0, occupancy=1.0):
                assigned_nodes.append(node)
                assigned_idxs.append(node_idx)
            if len(assigned_nodes) == num_nodes:
                break
        else:
            raise InsufficientResources
        spec = []
        for node in assigned_nodes:
            node.assign(job_id, num_cpus=0, num_gpus=0, occupancy=1.0)
            spec.append({"node_id": node.id, "hostname": node.hostname})
        self.job_node_map[job_id] = assigned_idxs
        return spec

    def assign(self, job_id, num_nodes, cpus_per_node, gpus_per_node, node_occupancy):
        if num_nodes > 1:
            return self._assign_multi_node(job_id, num_nodes)
        return self._assign_single_node(
            job_id, cpus_per_node, gpus_per_node, node_occupancy
        )

    def free(self, job_id):
        node_idxs = self.job_node_map.pop(job_id)
        for idx in node_idxs:
            node = self.nodes[idx]
            node.free(job_id)
