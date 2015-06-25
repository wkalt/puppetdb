events with distinct_resources:
    [extract [distinct [on certname resource_type resource_title property]]
      [extract * [= certname host-1] [order_by timestamp]]]

event-counts:
    [extract [[function count] resource_type resource_title status]
      [= certname host-1] [group_by resource_type resource_title status]]

event counts with distinct_resources:
    [extract [[function count] resource_type resource_title status]
      [extract [distinct [on certname resource_type resource_title property]]
        [extract * [= certname host-1] [order_by timestamp]]]
          [group_by resource_type resource_title status]]

event counts with count_by=certname:
    [extract [[function count] resource_type resource_title status]
      [extract [distinct certname status resource_type resource_title]
        [= certname host-1]]]

distinct event-counts with count_by=certname:
    [extract [[function count] resource_type resource_title status]
      [extract [distinct certname status resource_type resource_title]
        [extract [distinct [on certname resource_type resource_title property]]
          [extract * [= certname host-1] [order_by timestamp]]]]
            [group_by resource_type resource_title status]]

aggregate event counts:
    [extract [[function count] status]
      [extract [[function count] resource_type resource_title status]
        [= certname host-1] [group_by resource_type resource_title status]]
      [group_by status]]

distinct aggregate event counts:
    [extract [[function count] status]
      [extract [[function count] resource_type resource_title status]
        [extract [distinct [on certname resource_type resource_title property]]
          [extract * [= certname host-1] [order_by timestamp]]]
        [group_by resource_type resource_title property]]
      [group_by status]]

distinct aggregate event counts with count-by:
    [extract [[function count] status]
      [extract [[function count] resource_type resource_title status]
        [extract [distinct certname status resource_type resource_title]
          [= certname host-1]]
        [group_by resource_type resource_title status]]
      [group_by status]]

distinct aggregate event counts with count-by:
    [extract [[function count] status]
      [extract [[function count] resource_type resource_title status]
        [extract [distinct certname status resource_type resource]
          [extract [distinct [on certname resource_type resource_title property]]
            [extract * [= certname host-1] [order_by timestamp]]]]
        [group_by resource_type resource_title status]]
      [group_by status]]
