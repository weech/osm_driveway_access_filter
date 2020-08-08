use osmpbfreader::{Node, NodeId, OsmId, OsmObj, OsmPbfReader, Ref, Relation, Tags, Way};
use std::collections::{BTreeMap, HashSet};
use std::io::{prelude::*, BufReader, Write};
use std::path::Path;
use time::OffsetDateTime;
use xml::{common::XmlVersion, writer::EventWriter, writer::XmlEvent, EmitterConfig};

/// Parse a file copy-pasted from the Wiki that has
/// a big list of Amazon Logistics editors
fn parse_amazon_editors(path: &Path) -> HashSet<String> {
    // Read the file
    let file = std::fs::File::open(path).unwrap();
    let reader = BufReader::new(file);

    // Organize into a HashSet
    let mut set = HashSet::new();
    for line in reader.lines() {
        set.insert(line.unwrap().trim_end().to_string());
    }
    set
}

fn compare_vals(p: f64, min: &mut f64, max: &mut f64) {
    let omin = if p < *min { p } else { *min };
    let omax = if p > *max { p } else { *max };
    *min = omin;
    *max = omax;
}

fn get_bounds(data: &BTreeMap<OsmId, OsmObj>) -> [f64; 4] {
    let mut minlat = std::f64::INFINITY;
    let mut minlon = std::f64::INFINITY;
    let mut maxlat = std::f64::NEG_INFINITY;
    let mut maxlon = std::f64::NEG_INFINITY;
    for (_, item) in data.iter() {
        match item {
            OsmObj::Node(n) => {
                let lat = n.lat();
                compare_vals(lat, &mut minlat, &mut maxlat);
                let lon = n.lon();
                compare_vals(lon, &mut minlon, &mut maxlon);
            }
            // Only nodes matter since they are fundamental
            _ => (),
        }
    }
    [minlat, minlon, maxlat, maxlon]
}

/// The goal of this script is to remove access=private
/// from ways introduced by Amazon. The steps to accomplish this are:
/// 1. Iterate through all the ways in the PBF applying a filter.
///     The filter requirements are:
///     - Created by an Amazon Logistics employee
///     - Has the tags `service=driveway` and `access=private`
///     - Does not have a node that has tag `barrier=*`
/// 2. Output (somehow) to JOSM for manual review
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let amazon = parse_amazon_editors(std::path::Path::new("public_data/amazon.txt"));
    let file = std::fs::File::open("private_data/new-hampshire-latest-internal.osm.pbf")?;
    let mut reader = OsmPbfReader::new(file);
    let filtered = reader.get_objs_and_deps(|element| {
        element.is_way()
            && element.tags().contains("service", "driveway")
            && element.tags().contains("access", "private")
            && element.user().is_some()
            && amazon.contains(
                element
                    .user()
                    .as_ref()
                    .expect("Short-circuiting broke")
                    .as_str(),
            )
    })?;
    // Do a second pass to get the bad nodes
    let mut poison_nodes = HashSet::new();
    for (id, obj) in filtered.iter() {
        if let OsmObj::Node(n) = obj {
            if n.tags.contains_key("barrier") {
                poison_nodes.insert(id.node().expect("Broken unwrapping osmid"));
            }
        };
    }
    // Actually filter out the ways with bad nodes
    let (mut good_ways, good_node_ids): (Vec<_>, Vec<_>) = filtered
        .iter()
        .filter_map(|(_, obj)| {
            if let OsmObj::Way(w) = obj {
                if poison_nodes
                    .intersection(&w.nodes.iter().map(|x| *x).collect::<HashSet<_>>())
                    .next()
                    .is_none()
                {
                    Some((obj, &w.nodes))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unzip();
    let good_node_ids: HashSet<_> = good_node_ids.into_iter().flatten().collect();
    // Add the nodes back in
    let mut good_items: Vec<_> = filtered
        .iter()
        .filter_map(|(_, obj)| {
            if let OsmObj::Node(n) = obj {
                if good_node_ids.contains(&n.id) {
                    Some(obj)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();
    good_items.append(&mut good_ways);

    // Turn into an osm file
    let mut osmfile = std::fs::File::create("output.osm").unwrap();
    let mut writer = EmitterConfig::new()
        .perform_indent(true)
        .create_writer(&mut osmfile);
    writer
        .write(XmlEvent::StartDocument {
            version: XmlVersion::Version10,
            encoding: Some("UTF-8"),
            standalone: None,
        })
        .unwrap();
    writer
        .write(XmlEvent::start_element("osm").attr("version", "0.6"))
        .unwrap();
    let bounds = get_bounds(&filtered);
    writer
        .write(
            XmlEvent::start_element("bounds")
                .attr("minlat", &bounds[0].to_string())
                .attr("minlon", &bounds[1].to_string())
                .attr("maxlat", &bounds[2].to_string())
                .attr("maxlon", &bounds[3].to_string()),
        )
        .unwrap();
    writer.write(XmlEvent::end_element()).unwrap();
    for item in good_items {
        match item {
            OsmObj::Node(n) => {
                node_to_xml(&mut writer, n);
                tags_to_xml(&mut writer, &n.tags);
                writer.write(XmlEvent::end_element()).unwrap();
            }
            OsmObj::Way(w) => {
                way_to_xml(&mut writer, w);
                nd_to_xml(&mut writer, &w.nodes);
                tags_to_xml(&mut writer, &w.tags);
                writer.write(XmlEvent::end_element()).unwrap();
            }
            OsmObj::Relation(r) => {
                relation_to_xml(&mut writer, r);
                member_to_xml(&mut writer, &r.refs);
                tags_to_xml(&mut writer, &r.tags);
                writer.write(XmlEvent::end_element()).unwrap();
            }
        }
    }
    writer.write(XmlEvent::end_element()).unwrap();
    Ok(())
}

fn node_to_xml<W>(writer: &mut EventWriter<W>, node: &Node)
where
    W: Write,
{
    writer
        .write(
            XmlEvent::start_element("node")
                .attr("id", &node.id.0.to_string())
                .attr("lat", &node.lat().to_string())
                .attr("lon", &node.lon().to_string())
                .attr(
                    "user",
                    &node
                        .user()
                        .as_ref()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "uid",
                    &node.uid().map(|x| x.to_string()).unwrap_or("".to_string()),
                )
                .attr(
                    "visible",
                    &node
                        .visible()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "version",
                    &node
                        .version()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "changeset",
                    &node
                        .changeset()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "timestamp",
                    &node
                        .timestamp()
                        .map(|x| OffsetDateTime::from_unix_timestamp(x).format("%FT%H:%M:%SZ"))
                        .unwrap_or("".to_string()),
                ),
        )
        .unwrap()
}

fn way_to_xml<W>(writer: &mut EventWriter<W>, node: &Way)
where
    W: Write,
{
    writer
        .write(
            XmlEvent::start_element("way")
                .attr("id", &node.id.0.to_string())
                .attr(
                    "user",
                    &node
                        .user()
                        .as_ref()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "uid",
                    &node.uid().map(|x| x.to_string()).unwrap_or("".to_string()),
                )
                .attr(
                    "visible",
                    &node
                        .visible()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "version",
                    &node
                        .version()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "changeset",
                    &node
                        .changeset()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "timestamp",
                    &node
                        .timestamp()
                        .map(|x| OffsetDateTime::from_unix_timestamp(x).format("%FT%H:%M:%SZ"))
                        .unwrap_or("".to_string()),
                ),
        )
        .unwrap()
}

fn relation_to_xml<W>(writer: &mut EventWriter<W>, node: &Relation)
where
    W: Write,
{
    writer
        .write(
            XmlEvent::start_element("relation")
                .attr("id", &node.id.0.to_string())
                .attr(
                    "user",
                    &node
                        .user()
                        .as_ref()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "uid",
                    &node.uid().map(|x| x.to_string()).unwrap_or("".to_string()),
                )
                .attr(
                    "visible",
                    &node
                        .visible()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "version",
                    &node
                        .version()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "changeset",
                    &node
                        .changeset()
                        .map(|x| x.to_string())
                        .unwrap_or("".to_string()),
                )
                .attr(
                    "timestamp",
                    &node
                        .timestamp()
                        .map(|x| OffsetDateTime::from_unix_timestamp(x).format("%FT%H:%M:%SZ"))
                        .unwrap_or("".to_string()),
                ),
        )
        .unwrap()
}

fn nd_to_xml<W>(writer: &mut EventWriter<W>, nds: &[NodeId])
where
    W: Write,
{
    for id in nds.iter() {
        writer
            .write(XmlEvent::start_element("nd").attr("ref", &id.0.to_string()))
            .unwrap();
        writer.write(XmlEvent::end_element()).unwrap();
    }
}

fn tags_to_xml<W>(writer: &mut EventWriter<W>, tags: &Tags)
where
    W: Write,
{
    for (k, v) in tags.iter() {
        writer
            .write(
                XmlEvent::start_element("tag")
                    .attr("k", k.as_str())
                    .attr("v", v.as_str()),
            )
            .unwrap();
        writer.write(XmlEvent::end_element()).unwrap();
    }
}

fn member_to_xml<W>(writer: &mut EventWriter<W>, members: &[Ref])
where
    W: Write,
{
    for m in members.iter() {
        let (kind, id) = match m.member {
            OsmId::Node(x) => ("node", x.0),
            OsmId::Way(x) => ("way", x.0),
            OsmId::Relation(x) => ("relation", x.0),
        };
        writer
            .write(
                XmlEvent::start_element("member")
                    .attr("type", kind)
                    .attr("ref", &id.to_string())
                    .attr("role", m.role.as_str()),
            )
            .unwrap();
        writer.write(XmlEvent::end_element()).unwrap();
    }
}
