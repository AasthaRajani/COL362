package rel;

import iterator.RelIterator;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.log4j.Logger;

import java.util.List;

public interface PRel extends RelNode, RelIterator {
    
    static final Logger logger = Logger.getLogger(PRel.class);

    PProjectFilter copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType, RexNode condition);
}