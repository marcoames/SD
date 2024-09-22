import re

def parse_snapshot(file_path):
    snapshots = []
    with open(file_path, 'r') as file:
        content = file.read()
        
    # Split snapshots by the delimiter
    snapshot_blocks = content.strip().split('-------------------------')
    
    for block in snapshot_blocks:

        if block.strip():
            lines = block.strip().split('\n')
            snapshot_info = {
                'snapshot_id': None,
                'process_id': None, 
                'waiting': [],
                'process_state': None,
                'mensagens': {}
            }
            for line in lines:
                if 'Snapshot ID' in line:
                    match = re.match(r'Snapshot ID: (\d+)\s+Process ID: (\d+)\s+Logic Clock: (\d+)\s+Waiting: \[(.*?)\]\s+Process State: (\d+)', line)    
                    if match:
                        snapshot_info['snapshot_id'] = int(match.group(1))
                        snapshot_info['process_id'] = int(match.group(2))
                        snapshot_info['waiting'] = match.group(4)
                        snapshot_info['process_state'] = int(match.group(5))
                elif 'Canal' in line:
                    match = re.findall(r'Canal\[(\d+), (\d+)\] = \[(.*?)\]', line)
                    for channel_id, receiver_id, messages in match:
                        snapshot_info['mensagens'][(int(channel_id), int(receiver_id))] = messages.strip()

            snapshots.append(snapshot_info)
    
    return snapshots


def invariante_1(snapshots):
    """Invariante 1: No máximo um processo na SC."""
    num_in_sc = sum(1 for snapshot in snapshots if snapshot['process_state'] == 2)
    return num_in_sc <= 1

def invariante_2(snapshots):
    """Invariante 2: Se todos os processos estão em 'não quero a SC', 
    todos os waitings devem ser falsos e não deve haver mensagens em trânsito."""
    all_not_wanting_sc = all(snapshot['process_state'] == 0 for snapshot in snapshots)
    no_waitings = all(all(not waiting for waiting in snapshot['waiting']) for snapshot in snapshots)
    no_messages = all(all(len(messages) == 0 for messages in snapshot['mensagens'].values()) for snapshot in snapshots)

    return not all_not_wanting_sc or (no_waitings and no_messages)

def invariante_3(snapshots):
    """Invariante 3: Se um processo está marcado como waiting em p, 
    então p está na SC (inMX) ou quer a SC (wantMX)."""
    
    for snapshot in snapshots:
        waiting = snapshot['waiting']
        process_state = snapshot['process_state']

        for w in waiting:
            if w == 'true' and process_state == 0:
                return False

    return True

def avaliar_snapshot(snapshots):
    """Avalia um conjunto de snapshots para todas as invariantes."""
    snapshot_id = snapshots[0]['snapshot_id']
    if invariante_1(snapshots):
        if invariante_2(snapshots):
            if invariante_3(snapshots):
                print(f"Snapshot {snapshot_id} está consistente!")
            else:
                print(f"Invariante 3 violada no Snapshot {snapshot_id}")
        else:
            print(f"Invariante 2 violada no Snapshot {snapshot_id}")
    else:
        print(f"Invariante 1 violada no Snapshot {snapshot_id}")

def process_all_snapshots(grouped_snapshots):
    """Avalia invariantes de conjuntos de snapshots com mesmo índice em múltiplos arquivos."""
    for snap_list in grouped_snapshots:
        avaliar_snapshot(snap_list)


def group_snapshots_by_id(snapshot_files):
    """Group snapshots by their snapshot_id across multiple files."""
    all_snapshots = {}
    
    for file in snapshot_files:
        snapshots = parse_snapshot(file)
        for snapshot in snapshots:
            snap_id = snapshot['snapshot_id']
            if snap_id not in all_snapshots:
                all_snapshots[snap_id] = []
            all_snapshots[snap_id].append(snapshot)

    # Create a list where each index corresponds to the snapshot IDs
    max_id = max(all_snapshots.keys())
    grouped_snapshots = [all_snapshots.get(i, []) for i in range(max_id + 1)]

    print_snapshots(grouped_snapshots)

    return grouped_snapshots


def print_snapshots(grouped_snapshots):
    for index, snapshots in enumerate(grouped_snapshots):
        if snapshots:
            print(f"\nSnapshots ID: {index}")
            for snapshot in snapshots:
                for key, value in snapshot.items():
                    print(f"    '{key}': {value},")
                print("\n")     
            
def main():
    snapshot_files = ['snapshot_0.txt', 'snapshot_1.txt', 'snapshot_2.txt']
    grouped_snapshots = group_snapshots_by_id(snapshot_files)
    process_all_snapshots(grouped_snapshots)
    

if __name__ == "__main__":
    main()