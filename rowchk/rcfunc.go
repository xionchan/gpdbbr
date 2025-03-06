package rowchk

// 校验数据
func checkdata(ma, mb map[string]float64) (onlya []string, onlyb []string, diffkey []DiffRowS) {
	mbKeys := make(map[string]struct{})
	for k := range mb {
		mbKeys[k] = struct{}{}
	}

	for k, aVal := range ma {
		if _, ok := mb[k]; !ok {
			onlya = append(onlya, k)
		} else {
			if aVal != mb[k] {
				diffkey = append(diffkey, DiffRowS{
					TabName: k,
					BkRow:   aVal,
					DbRow:   mb[k],
				})
			}
		}
	}

	for k := range mb {
		if _, ok := ma[k]; !ok {
			onlyb = append(onlyb, k)
		}
	}

	return
}
