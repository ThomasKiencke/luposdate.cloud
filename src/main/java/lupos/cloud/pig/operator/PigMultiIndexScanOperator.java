//package lupos.cloud.pig.operator;
//
//import java.util.ArrayList;
//import java.util.HashSet;
//
//import lupos.cloud.operator.IndexScanContainer;
//import lupos.cloud.operator.MultiIndexScanContainer;
//import lupos.cloud.operator.format.IndexScanCointainerFormatter;
//import lupos.cloud.pig.JoinInformation;
//import lupos.cloud.pig.PigQuery;
//import lupos.engine.operators.BasicOperator;
//
//public class PigMultiIndexScanOperator implements IPigOperator {
//	private boolean debug;
//	private JoinInformation newJoin;
//	private ArrayList<JoinInformation> multiInputist;
//	private MultiIndexScanContainer multiIndexScanContainer = null;
//	
//	
//	public PigMultiIndexScanOperator(JoinInformation newJoin, ArrayList<JoinInformation> multiInputist) {
//		this.newJoin = newJoin;
//		this.multiInputist = multiInputist;
//	}
//	public String buildQuery(PigQuery pigQuery) {
//		this.joinMultiIndexScans(multiIndexScanContainer, pigQuery);
//		return result.toString();
//	}
//	
//	public JoinInformation joinMultiIndexScans(MultiIndexScanContainer container, PigQuery pigQuery ) {
//		JoinInformation newJoin = null;
//		for (Integer id : container.getContainerList().keySet()) {
//			HashSet<BasicOperator> curList = container.getContainerList().get(
//					id);
//			ArrayList<JoinInformation> multiInputist = new ArrayList<JoinInformation>();
//			for (BasicOperator op : curList) {
//				if (op instanceof IndexScanContainer) {
//					new IndexScanCointainerFormatter().serialize(op, pigQuery);
//
//				} else if (op instanceof MultiIndexScanContainer) {
//					final MultiIndexScanContainer c = (MultiIndexScanContainer) op;
//					this.joinMultiIndexScans(c, pigQuery);
//				}
//			}
//
//			newJoin = new JoinInformation();
//			if (container.getMappingTree().get(id) instanceof Union) {
//				pigQuery.buildAndAppendQuery(new PigUnionOperator(newJoin,
//						multiInputist));
//			} else if (container.getMappingTree().get(id) instanceof Join) {
//				pigQuery.buildAndAppendQuery(new PigJoinOperator(newJoin,
//						multiInputist, (Join) container.getMappingTree()
//								.get(id)));
//			} else {
//				throw new RuntimeException(
//						"Something is wrong here. Forgot case? -> "
//								+ container.getMappingTree().get(id).getClass());
//			}
//
//			HashSet<String> variables = new HashSet<String>();
//			for (JoinInformation toRemove : multiInputist) {
//				variables.addAll(toRemove.getJoinElements());
//				this.intermediateBags.remove(toRemove);
//			}
//
//			newJoin.setJoinElements(new ArrayList<String>(variables));
//			this.intermediateBags.add(newJoin);
//
//		}
//		return newJoin; // never reached
//	}
//}
