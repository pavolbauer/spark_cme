def importMapper(line):
    """ divide a string into key/value pairs """
    fields=line.split(' ');
    value=float(fields[1]);
    return (fields[0],value); 

def isfeasible(state):
    """ check if the state is feasible (has a positive probability) """
    for s in state:
        if s < 0:
            return False   
    return True

def expand_state(input):
    """ Expand around a state and compute the values of the new states. 
        Form the string for these new states. """
    
    state_string=input[0];
    old_state_val=input[1];
    
    numbers=state_string.split(',');
    state=map(int, numbers);
    
    # Time step of the solver
    dt = 0.001
    
    ka  = 0.3
    kb  = 0.3
    Ki  = 60.0
    k2  = 0.001
    mu  = 0.002
    kr  = 30.0
    kea = 0.02
    keb = 0.02
    
    state_val = 0.0
    
    reac_string = ""
    cnt=0;
    new_tuppel=[]
    
    # Forward Euler
    val = k2*state[0]*state[1]*dt*old_state_val
    new_state = state[:]
    new_state[0] -= 1
    new_state[1] -= 1
    
    if isfeasible(new_state):
        state_val += val
        reac_string = '%s' % (",".join(str(e) for e in new_state))
        new_tuppel.append((reac_string, val))
    
    val = mu*state[0]*dt*old_state_val
    new_state = state[:]
    new_state[0] -= 1
    
    if isfeasible(new_state):
        state_val += val
        reac_string = '%s' % (",".join(str(e) for e in new_state))
        new_tuppel.append((reac_string, val))

    val = ka*state[2]/(1.0+state[0]/Ki)*dt*old_state_val
    new_state = state[:]
    new_state[0] += 1
    
    if isfeasible(new_state):
        state_val += val
        reac_string = '%s' % (",".join(str(e) for e in new_state))
        new_tuppel.append((reac_string, val))

    val = mu*state[1]*dt*old_state_val
    new_state = state[:]
    new_state[1] -= 1
    
    if isfeasible(new_state):
        state_val += val
        reac_string = '%s' % (",".join(str(e) for e in new_state))
        new_tuppel.append((reac_string, val))

    reac_string = '%s' % (",".join(str(e) for e in state))        
    new_tuppel.append((reac_string, old_state_val - state_val))

    return new_tuppel

# initial state import
lines=sc.parallelize(["10,10,10 1.0"],1);
states = lines.flatMap(lambda x: x.split('\n')) 

# map the words into (word,1) tuples
stateTuples = states.map(importMapper) 
stateTuples.getNumPartitions()

# cache the initial state
ITERATIONS=200 
stateTuples.cache()
stateTuples.getStorageLevel()

# run the computation for 
for i in range(1, ITERATIONS):
    stateTuples=stateTuples.map(expand_state) \
        .flatMap(lambda line: line) \
        .reduceByKey(lambda x, y: x+y)
stateTuples.values().count()

# final statistics
stateTuples.stats()
