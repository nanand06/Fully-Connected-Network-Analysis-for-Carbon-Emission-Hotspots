
use std::io;


mod dataprocessing;
mod graph;

use graph::SpatialGraph;
use dataprocessing::DataFrame;


fn main() {

    //prompts user to entire specific year
    println!("Please enter a specific year you're interested in anazlying Ex. 2000. Allowed years include(1970-2021");
    let mut year = String::new();
    io::stdin().read_line(&mut year).expect("Error: Could not read line");
    let parsed_year : i64 = year.trim().parse::<i64>().unwrap();

    // using known cloumn types, read the csv
    let mut df = DataFrame::new(5);
    let column_labels = vec![4, 1, 1, 1, 3];
    let _ = df.read_csv("src/data/emissions.csv", &column_labels);

    // perform the data filtering, graph initialization, Dijkstra's, and closeness calculation and output to terminal
    let spatial_graph = SpatialGraph::new(&df, parsed_year);
    let (adj_lst_encoded, node_index)= spatial_graph.one_hot_encode_adjacency_list();
    let closeness_centrality_vector = spatial_graph.calculate_closeness(adj_lst_encoded.clone());
    for (node, index) in node_index.iter() {
        println!("{:?}, Closeness Centrality: {:?}", node, closeness_centrality_vector[*index]);
    }
}

// test is run on small sized dataset with manually calculated correct closeness values for comparison
#[test]
fn verify_centrality() {
    // same logic as main func but reads form the small file.
    let mut df = DataFrame::new(5);
    let column_labels = vec![4, 1, 1, 1, 3];
    let result_enum = df.read_csv("src/data/small_emissions.csv", &column_labels);

    println!("{:?}", df.columns);
    let spatial_graph = SpatialGraph::new(&df, 1970);
    let (adj_lst_encoded, node_index)= spatial_graph.one_hot_encode_adjacency_list();
    let closeness_centrality_vector = spatial_graph.calculate_closeness(adj_lst_encoded.clone());
    let epsilon = 0.000001;
    let mut pass: bool = true;
    for (node, index) in node_index.iter() {
        let centrality = closeness_centrality_vector[*index];
        println!("{:?}, Closeness Centrality: {:?}", node, centrality);
        if node.location == String::from("Arizona") {// check if centrality value is within reasonable range
            if !(centrality - epsilon < 0.02190629 && centrality + epsilon > 0.02190629) {
                pass = false;
            }
        }
        if node.location == String::from("Alabama") {
            if !(centrality - epsilon < 0.0118316 && centrality + epsilon > 0.0118316) {
                pass = false;
            }
        }
        if node.location == String::from("Alaska") {
            if !(centrality - epsilon < 0.019073919 && centrality + epsilon > 0.019073919) {
                pass = false;
            }
        }
    }
    assert_eq!(pass, true);
}