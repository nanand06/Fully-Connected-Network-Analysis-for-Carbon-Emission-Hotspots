
use std::collections::HashMap;
use std::hash::Hasher;
use std::hash::Hash;
use std::collections::HashSet;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

type AjacencyLists = HashMap<Node, Vec<(Node, Edge)>>;
type ListOfEdges = Vec<(Node, Node, f64)>; 
type Edge = f64;// third entry is edge weight

use crate::dataprocessing::ColumnVal;
use crate::dataprocessing::DataFrame;

/*The purpose of this module is to filter the data(clean), create the graph from my
 dataset, implement Dijkstra's algorithm, and calculate closeness centrality*/

#[derive(Debug, Clone)]// representation of a node in my grpah
pub struct Node {
    pub location: String, 
    year: i64, 
    carbon_emission: f64, 

}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.location == other.location && self.year == other.year
    }
}
impl Eq for Node {}

impl Hash for Node {// had to override the traits since each node has an f64 emission value 
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.location.hash(state);
        self.year.hash(state);
    }
}
#[derive(Copy, Clone, PartialEq)]
struct State {// a state struct for Dijkstra's algorithm for ease
    cost: f64, 
    position: usize,
}
impl Eq for State {}

impl Ord for State {// reversing this so i can get a min heap
    fn cmp(&self, other: &Self) -> Ordering {
        
        other.cost.partial_cmp(&self.cost).unwrap()
    }
}
impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}



#[derive(Debug, Clone)]
pub struct SpatialGraph {// Graphical represnetaiton of my dataset
    adjacency_list : AjacencyLists, 
}
impl SpatialGraph {
    /* new function takes a dataframe and a current year input from terminal
    and rates an edge list and adjacency list. The necessary filtering is done here as stated in the writeup, 
    to narrow down the type of emission values.
    */
    pub fn new(df: &DataFrame, curr_year: i64) -> SpatialGraph { 
        let mut list_of_edges : ListOfEdges = vec![];
        for column in df.columns.iter() {
            
            if column.as_ref().unwrap().label == String::from("year") { // catch the year column and iterate row-wise thorugh that
                for row_num in 0..column.as_ref().unwrap().data.len(){
                    let mut node : Node;
                    let mut update = false;
              
                    node = Node {year: 0, location: String::from(""), carbon_emission: 0.0};
                    if let ColumnVal::Four(year) = column.as_ref().unwrap().data[row_num] {
                        if year == curr_year { // check if years match
                            let curr_location = df.columns[1].as_ref().unwrap().data[row_num].clone();
                            if let ColumnVal::One(name) = curr_location { // extract location
                                let emission = df.columns[4].as_ref().unwrap().data[row_num].clone();
                                if let ColumnVal::Three(carbon) = emission {// extract emission value
                                    let sector_name = df.columns[2].as_ref().unwrap().data[row_num].clone();
                                    let fuel_type = df.columns[3].as_ref().unwrap().data[row_num].clone();
                                    if let ColumnVal::One(s_name) = sector_name { // extract sector name
                                        if s_name == String::from("Total carbon dioxide emissions from all sectors"){ 
                                        if let ColumnVal::One(f_type) = fuel_type { // extral fuel type
                                            if f_type == String::from("All Fuels") {
                                                update = true;
                                                node = Node{year: curr_year, location: name, carbon_emission: carbon};
                                                
                                                }
                                            }
                                        }
                                    }
                                    
                                }
                            }
                        }
                    }
                    if update { // Im creatd a complete graph, so i terate through all combinations which requires a nested loop
                        for j in (row_num + 1)..column.as_ref().unwrap().data.len() {
                            let mut update2 = false;
                            let mut node2 : Node;
                            node2 = Node {year: 0, location: String::from(""), carbon_emission: 0.0};
                            if let ColumnVal::Four(year) = column.as_ref().unwrap().data[j] {
                                if year == curr_year {
                                    let curr_location = df.columns[1].as_ref().unwrap().data[j].clone();
                                    if let ColumnVal::One(name) = curr_location {
                                        let emission = df.columns[4].as_ref().unwrap().data[j].clone();
                                        if let ColumnVal::Three(carbon) = emission {
                                            let sector_name = df.columns[2].as_ref().unwrap().data[j].clone();
                                            let fuel_type = df.columns[3].as_ref().unwrap().data[j].clone();
                                            if let ColumnVal::One(s_name) = sector_name {
                                                if s_name == String::from("Total carbon dioxide emissions from all sectors") { 
                                                    if let ColumnVal::One(f_type) = fuel_type {
                                                        if f_type == String::from("All Fuels") {
                                                            update2 = true;
                                                            node2 = Node{year: curr_year, location: name, carbon_emission: carbon};
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            let carbon_emission_diff : f64 = (node.carbon_emission - node2.carbon_emission).abs();
                            if update == true && update2 == true { // if the rows in the dataset are valid and I care about them for analysis
                                list_of_edges.push((node.clone(), node2.clone(), carbon_emission_diff));
                            }
                        } 
                    }
                }
            }
        }
        let mut adj_list : AjacencyLists = HashMap::new();
        for (from, to, diff) in &list_of_edges {// create adjacency list
            adj_list.entry(from.clone()).or_default().push((to.clone(), *diff));
            adj_list.entry(to.clone()).or_default().push((from.clone(), *diff));
            
        }

        return SpatialGraph { adjacency_list: adj_list};
    }

    // This function encodes the Nodes to indexes(unique IDs such that Dijkstra's is easier to implement
    pub fn one_hot_encode_adjacency_list(&self) -> (Vec<Vec<(usize, f64)>>, HashMap<Node, usize>) {
     // outputs the adjacency list and the node mapping of ids   

        let mut unique_nodes = HashSet::new();

        
        for (key, _) in &self.adjacency_list {
            unique_nodes.insert(key.clone());
        }
        let mut  node_index = HashMap::new();

        for (index, node) in unique_nodes.iter().enumerate() {
            node_index.insert(node.clone(), index);
        }
        let mut final_vec = vec![vec![];unique_nodes.len()];

        for (node, neighbors) in self.adjacency_list.iter() {
            let u = node_index[node];

            for (node2, weight) in neighbors.iter() {
                let v =node_index[node2];
                final_vec[u].push((v, *weight));
            }
        }
        return (final_vec, node_index);

 
    }
    // takes in a starting point and adjacency list and runs Dijkstras algorithm, and outputs a distance vector
    pub fn implement_djistras(&self, start: usize, jlst: Vec<Vec<(usize, f64)>>) -> Vec<f64> {
        let mut dist = vec![10000.0;self.adjacency_list.len()];
        dist[start] = 0.0;

        let mut heap = BinaryHeap::new();

        heap.push(State{cost: 0.0, position: start});

        while let Some(State {cost, position }) = heap.pop() { // while min heap not empty, pop
            if cost > dist[position] {
                continue; // if cost greater then existing path, move on
            }
    
            for &(neighbor, weight) in &jlst[position] {// iterate through all neighbors, update the distance if less
                let next_cost = cost + weight;
                if next_cost < dist[neighbor] {
                    dist[neighbor] = next_cost;
                    heap.push(State { cost: next_cost, position: neighbor });// push neighbors to heap
                }
            }
        }
        return dist;
    }
    // takes in adjacency list and calculates the closeness centrality for each node.
    pub fn calculate_closeness(&self, lst: Vec<Vec<(usize, f64)>>) -> Vec<f64>{
        let mut closeness = vec![];
        for i in 0..self.adjacency_list.len() { 
            let distances = self.implement_djistras(i, lst.clone());
            let mut dist_sum = 0.0;
            let mut reachable = -1.0;

            for d in distances.iter() {
                dist_sum += d;
                reachable += 1.0;
            }
            closeness.push(reachable/dist_sum);
        }
        return closeness;
    }
    
}